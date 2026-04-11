import fetch from 'node-fetch';
import { writeFile } from 'fs/promises';
import * as dotenv from 'dotenv';
dotenv.config();

const CLIENT_ID = process.env.BOL_CLIENT_ID;
const CLIENT_SECRET = process.env.BOL_CLIENT_SECRET;

// Centrale rate-limiter: max 1 call per 200ms (~300/min), retry bij 429
let lastCallTime = 0;
async function bolFetch(url, options = {}) {
  const wacht = Math.max(0, 200 - (Date.now() - lastCallTime));
  if (wacht > 0) await new Promise(r => setTimeout(r, wacht));
  lastCallTime = Date.now();

  const res = await fetch(url, options);

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get('Retry-After') || '10', 10);
    console.log(`⏸️  Rate limit — wacht ${retryAfter}s...`);
    await new Promise(r => setTimeout(r, retryAfter * 1000));
    return bolFetch(url, options);
  }

  return res;
}

// Stap 1: Access token ophalen
async function getAccessToken() {
  const credentials = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
  const response = await fetch('https://login.bol.com/token?grant_type=client_credentials', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${credentials}`,
      'Accept': 'application/json',
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Token ophalen mislukt (${response.status}): ${text}`);
  }
  const data = await response.json();
  if (!data.access_token) {
    throw new Error(`Geen access_token in response: ${JSON.stringify(data)}`);
  }
  return data.access_token;
}

// Stap 2: Alle eigen aanbiedingen ophalen via CSV export (1 bulk call)
async function getEigenaanbiedingen(token) {
  const response = await bolFetch('https://api.bol.com/retailer/offers/export', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
      'Content-Type': 'application/vnd.retailer.v10+json',
    },
    body: JSON.stringify({ format: 'CSV' }),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Offers export mislukt (${response.status}): ${text}`);
  }
  const data = await response.json();
  return data.processStatusId;
}

// Stap 3: Wacht op export en haal CSV op
async function wachtOpExport(token, processStatusId) {
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 5000));
    const res = await bolFetch(`https://api.bol.com/shared/process-status/${processStatusId}`, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept': 'application/vnd.retailer.v10+json',
      },
    });
    const status = await res.json();
    console.log(`⏳ Poging ${i + 1}: status = ${status.status}`);

    if (status.status === 'SUCCESS') {
      const csvRes = await bolFetch(`https://api.bol.com/retailer/offers/export/${status.entityId}`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept': 'application/vnd.retailer.v10+csv',
        },
      });
      return await csvRes.text();
    }

    if (status.status === 'FAILURE') {
      throw new Error('Export mislukt bij bol.com');
    }
  }
  throw new Error('Export duurde te lang');
}

// Stap 4: CSV parsen naar lijst van aanbiedingen
function parseCsv(csv) {
  const regels = csv.trim().split('\n');
  const headers = regels[0].split(',').map(h => h.replace(/"/g, '').trim());
  return regels.slice(1).map(regel => {
    const waarden = regel.split(',').map(w => w.replace(/"/g, '').trim());
    return Object.fromEntries(headers.map((h, i) => [h, waarden[i]]));
  });
}

// Stap 5: Concurrent-aanbiedingen ophalen per EAN (1 call per product)
async function getConcurrenten(token, ean) {
  const res = await bolFetch(`https://api.bol.com/retailer/products/${ean}/offers?country=NL`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
    },
  });
  if (!res.ok) return { offers: [] };
  return await res.json();
}

// Stap 6: Productnaam ophalen via products/list (searchTerm = EAN)
// Alleen aangeroepen voor producten met goedkopere concurrent
async function getProductNaam(token, ean, fallback) {
  const res = await bolFetch('https://api.bol.com/retailer/products/list', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
      'Content-Type': 'application/vnd.retailer.v10+json',
    },
    body: JSON.stringify({ countryCode: 'NL', searchTerm: ean }),
  });
  if (!res.ok) return fallback;
  const data = await res.json();
  return data.products?.[0]?.title || fallback;
}

// Stap 7: Retailernaam ophalen met cache
const retailerCache = new Map();
async function getRetailerNaam(token, retailerId) {
  if (retailerCache.has(retailerId)) return retailerCache.get(retailerId);
  const res = await bolFetch(`https://api.bol.com/retailer/retailers/${retailerId}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
    },
  });
  const naam = res.ok ? (await res.json()).displayName || retailerId : String(retailerId);
  retailerCache.set(retailerId, naam);
  return naam;
}

// Hoofdprogramma
async function main() {
  const testEan = process.env.TEST_EAN || null;
  console.log('🎣 Bol.com prijsmonitor gestart...\n');
  if (testEan) console.log(`🧪 Testmodus: alleen EAN ${testEan}\n`);

  const token = await getAccessToken();
  console.log('✅ Ingelogd bij bol.com API');

  const processStatusId = await getEigenaanbiedingen(token);
  const csv = await wachtOpExport(token, processStatusId);
  let aanbiedingen = parseCsv(csv);
  if (testEan) aanbiedingen = aanbiedingen.filter(a => a['ean'] === testEan);
  console.log(`📦 ${aanbiedingen.length} eigen aanbiedingen gevonden\n`);
  console.log('🔍 Concurrent-prijzen ophalen (±1 call/product)...\n');

  const kandidaten = [];

  // Fase 1: concurrent-prijzen ophalen voor alle producten
  for (const aanbieding of aanbiedingen) {
    const ean = aanbieding['ean'];
    const eigenPrijs = parseFloat(aanbieding['bundlePricesPrice']);
    const referentie = aanbieding['referenceCode'] || ean;

    if (!ean || isNaN(eigenPrijs)) continue;

    const concurrentData = await getConcurrenten(token, ean);
    const offers = concurrentData.offers || [];
    const goedkopere = offers.filter(o => o.price && o.price < eigenPrijs && !o.bestOffer);

    if (goedkopere.length > 0) {
      kandidaten.push({ ean, eigenPrijs, referentie, goedkopere });
    }
  }

  console.log(`\n💡 ${kandidaten.length} product(en) met goedkopere concurrent — namen en retailers ophalen...\n`);

  // Fase 2: namen + retailernamen alleen ophalen voor producten met goedkopere concurrent
  const rapport = [];
  for (const { ean, eigenPrijs, referentie, goedkopere } of kandidaten) {
    const productNaam = await getProductNaam(token, ean, referentie);
    const laagste = Math.min(...goedkopere.map(o => o.price));
    const concurrentenMetNaam = [];
    for (const o of goedkopere) {
      concurrentenMetNaam.push({
        retailerNaam: await getRetailerNaam(token, o.retailerId),
        retailerId: o.retailerId,
        prijs: o.price,
        verschil: parseFloat((eigenPrijs - o.price).toFixed(2)),
        fulfilment: o.fulfilmentMethod,
      });
    }
    rapport.push({
      product: productNaam,
      ean,
      eigenPrijs,
      laagsteConcurrent: laagste,
      verschil: parseFloat((eigenPrijs - laagste).toFixed(2)),
      concurrenten: concurrentenMetNaam,
    });
  }

  rapport.sort((a, b) => b.verschil - a.verschil);

  // Rapport printen
  console.log('═══════════════════════════════════════════════');
  console.log('📊 DAGELIJKS PRIJSRAPPORT BOL.COM');
  console.log(`📅 ${new Date().toLocaleDateString('nl-NL')}`);
  console.log('═══════════════════════════════════════════════\n');

  if (rapport.length === 0) {
    console.log('✅ Niemand zit onder jouw prijs. Goed bezig!');
  } else {
    console.log(`⚠️  ${rapport.length} product(en) hebben een goedkopere concurrent:\n`);
    for (const r of rapport) {
      console.log(`📦 ${r.product} (${r.ean})`);
      console.log(`   Jouw prijs:         €${r.eigenPrijs.toFixed(2)}`);
      console.log(`   Laagste concurrent: €${r.laagsteConcurrent.toFixed(2)}`);
      console.log(`   Verschil:           €${r.verschil.toFixed(2)}`);
      for (const c of r.concurrenten) {
        console.log(`   └ ${c.retailerNaam} (${c.fulfilment}): €${c.prijs.toFixed(2)}`);
      }
      console.log();
    }
  }

  const uitvoer = {
    gegenereerd: new Date().toISOString(),
    aantalProducten: rapport.length,
    producten: rapport,
  };
  await writeFile('rapport.json', JSON.stringify(uitvoer, null, 2), 'utf-8');
  console.log('💾 Rapport opgeslagen als rapport.json');
}

main().catch(console.error);
