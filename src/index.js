import fetch from 'node-fetch';
import { writeFile, mkdir } from 'fs/promises';
import ExcelJS from 'exceljs';
import * as dotenv from 'dotenv';
dotenv.config();

const CLIENT_ID = process.env.BOL_CLIENT_ID;
const CLIENT_SECRET = process.env.BOL_CLIENT_SECRET;

// Centrale rate-limiter: max 1 call per 100ms (~600/min), retry bij 429
let lastCallTime = 0;
async function bolFetch(url, options = {}) {
  const wacht = Math.max(0, 100 - (Date.now() - lastCallTime));
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
    process.stdout.write('.');

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
  console.log(`📦 ${aanbiedingen.length} aanbiedingen — concurrent-prijzen ophalen...`);

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

  console.log(`💡 ${kandidaten.length} product(en) goedkoper — namen ophalen...`);

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

  const nu = new Date();
  const datumLabel = nu.toISOString().slice(0, 10); // bijv. 2026-04-11

  // JSON rapport (overschrijven)
  const uitvoer = {
    gegenereerd: nu.toISOString(),
    aantalProducten: rapport.length,
    producten: rapport,
  };
  await writeFile('rapport.json', JSON.stringify(uitvoer, null, 2), 'utf-8');

  // Excel rapport met datum in bestandsnaam
  await mkdir('rapporten', { recursive: true });
  const excelPad = `rapporten/prijsrapport-${datumLabel}.xlsx`;

  const werkboek = new ExcelJS.Workbook();
  werkboek.creator = 'Bol Prijsmonitor';
  werkboek.created = nu;

  const blad = werkboek.addWorksheet('Prijsrapport');
  blad.columns = [
    { header: 'Product',             key: 'product',        width: 40 },
    { header: 'EAN',                 key: 'ean',             width: 16 },
    { header: 'Eigen prijs (€)',     key: 'eigenPrijs',      width: 16 },
    { header: 'Laagste concurrent (€)', key: 'laagste',     width: 22 },
    { header: 'Verschil (€)',        key: 'verschil',        width: 14 },
    { header: 'Concurrent',          key: 'retailerNaam',    width: 28 },
    { header: 'Concurrent prijs (€)', key: 'concPrijs',     width: 22 },
    { header: 'Fulfilment',          key: 'fulfilment',      width: 12 },
  ];

  // Koprij opmaken
  blad.getRow(1).font = { bold: true };
  blad.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0056A3' } };
  blad.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };

  for (const r of rapport) {
    for (const c of r.concurrenten) {
      blad.addRow({
        product:     r.product,
        ean:         r.ean,
        eigenPrijs:  r.eigenPrijs,
        laagste:     r.laagsteConcurrent,
        verschil:    r.verschil,
        retailerNaam: c.retailerNaam,
        concPrijs:   c.prijs,
        fulfilment:  c.fulfilment,
      });
    }
  }

  // Prijskolommen als valuta opmaken
  for (const col of ['eigenPrijs', 'laagste', 'verschil', 'concPrijs']) {
    blad.getColumn(col).numFmt = '€#,##0.00';
  }

  await werkboek.xlsx.writeFile(excelPad);

  console.log(`✅ ${rapport.length} product(en) met goedkopere concurrent`);
  console.log(`📊 Excel: ${excelPad}`);
  console.log(`📄 JSON:  rapport.json`);
}

main().catch(console.error);
