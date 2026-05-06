import fetch from 'node-fetch';
import { writeFile, readFile, mkdir } from 'fs/promises';
import ExcelJS from 'exceljs';
import * as dotenv from 'dotenv';
dotenv.config();

const COUNTRY = (process.env.COUNTRY || 'NL').toUpperCase();
const CLIENT_ID = process.env[`BOL_CLIENT_ID_${COUNTRY}`] || (COUNTRY === 'NL' ? process.env.BOL_CLIENT_ID : null);
const CLIENT_SECRET = process.env[`BOL_CLIENT_SECRET_${COUNTRY}`] || (COUNTRY === 'NL' ? process.env.BOL_CLIENT_SECRET : null);
const TEST_MODE = process.env.TEST_MODE === 'true';

const RAPPORT_PAD    = `rapport-${COUNTRY}.json`;
const NAAM_CACHE_PAD = `naam-cache-${COUNTRY}.json`;

// Vaste testartikelen (noodback als rapport.json nog geen offer IDs bevat)
const TESTARTIKELEN_FALLBACK = [
  {
    product: 'Zebra F-301 Balpen Zwart – 12 stuks',
    ean: '4901681021963',
    offerId: 'test-offer-001',
    eigenPrijs: 9.95,
    laagsteConcurrent: 8.49,
    verschil: 1.46,
    concurrenten: [
      { retailerNaam: 'Kantoorland BV',  retailerId: 'T001', prijs: 8.49, verschil: 1.46, fulfilment: 'FBR' },
      { retailerNaam: 'OfficeShop.nl',   retailerId: 'T002', prijs: 8.99, verschil: 0.96, fulfilment: 'FBB' },
    ],
  },
  {
    product: 'Post-it Super Sticky Notes 76×76mm – 12 blokken',
    ean: '0051141909784',
    offerId: 'test-offer-002',
    eigenPrijs: 14.99,
    laagsteConcurrent: 13.45,
    verschil: 1.54,
    concurrenten: [
      { retailerNaam: 'Paperworld.nl', retailerId: 'T003', prijs: 13.45, verschil: 1.54, fulfilment: 'FBR' },
    ],
  },
  {
    product: 'Maped Comfort Schaar 21 cm – Ergonomisch',
    ean: '3154147468509',
    offerId: 'test-offer-003',
    eigenPrijs: 6.49,
    laagsteConcurrent: 5.99,
    verschil: 0.50,
    concurrenten: [
      { retailerNaam: 'Knutselpaleis',       retailerId: 'T004', prijs: 5.99, verschil: 0.50, fulfilment: 'FBB' },
      { retailerNaam: 'Schoolspullen.nl',    retailerId: 'T005', prijs: 6.25, verschil: 0.24, fulfilment: 'FBR' },
    ],
  },
];

const TESTVEILIG_FALLBACK = [
  {
    product: 'Staedtler Mars Lumograph Potlood HB – Set 12 stuks',
    ean: '4007817321065',
    offerId: 'test-offer-004',
    eigenPrijs: 4.95,
    dichtstbijzijneConcurrent: 5.29,
    marge: 0.34,
  },
  {
    product: 'Bic Cristal Balpen Blauw – 50 stuks',
    ean: '3086123397216',
    offerId: 'test-offer-005',
    eigenPrijs: 12.99,
    dichtstbijzijneConcurrent: 14.50,
    marge: 1.51,
  },
];

// Centrale rate-limiter: 100ms tussenpoos = exact 10 req/sec (bol.com limiet)
let lastCallTime = 0;
async function bolFetch(url, options = {}) {
  const wacht = Math.max(0, 100 - (Date.now() - lastCallTime));
  if (wacht > 0) await new Promise(r => setTimeout(r, wacht));
  lastCallTime = Date.now();

  const res = await fetch(url, options);

  if (res.status === 429) {
    process.stdout.write('⏸');
    await new Promise(r => setTimeout(r, 2000));
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
  const res = await bolFetch(`https://api.bol.com/retailer/products/${ean}/offers?country=${COUNTRY}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
    },
  });
  if (!res.ok) return { offers: [] };
  return await res.json();
}

// Stap 6: Productnaam ophalen — met dagelijkse cache in naam-cache.json
let naamCache = {};
let naamCacheNieuwSindsOpslaan = 0;
const CACHE_TUSSENTIJDS_INTERVAL = 50;

async function laadNaamCache() {
  try {
    naamCache = JSON.parse(await readFile(NAAM_CACHE_PAD, 'utf-8'));
    console.log(`📖 Naam-cache: ${Object.keys(naamCache).length} namen geladen`);
  } catch {
    naamCache = {};
  }
}

const CACHE_DAGEN = 7;

async function getProductNaam(token, ean, fallback) {
  const gecached = naamCache[ean];
  if (gecached) {
    const ouderDan = (Date.now() - gecached.ts) / 86_400_000;
    if (ouderDan < CACHE_DAGEN) return gecached.naam;
  }
  const res = await bolFetch('https://api.bol.com/retailer/products/list', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
      'Content-Type': 'application/vnd.retailer.v10+json',
    },
    body: JSON.stringify({ countryCode: COUNTRY, searchTerm: ean }),
  });
  if (!res.ok) return fallback;
  const data = await res.json();
  const naam = data.products?.[0]?.title || fallback;
  naamCache[ean] = { naam, ts: Date.now() };
  naamCacheNieuwSindsOpslaan++;
  if (naamCacheNieuwSindsOpslaan % CACHE_TUSSENTIJDS_INTERVAL === 0) {
    await writeFile(NAAM_CACHE_PAD, JSON.stringify(naamCache), 'utf-8');
    process.stdout.write('💾');
  }
  return naam;
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
  console.log(`🎣 Bol.com prijsmonitor gestart (${COUNTRY})...\n`);

  if (TEST_MODE) {
    console.log('🧪 Testmodus actief — bestaand rapport wordt gebruikt als bron.\n');

    let testProducten = TESTARTIKELEN_FALLBACK;
    let testVeilig    = TESTVEILIG_FALLBACK;

    try {
      const bestaand = JSON.parse(await readFile(RAPPORT_PAD, 'utf-8'));
      const echteProducten = (bestaand.producten || []).filter(p => p.offerId && !p.offerId.startsWith('test-'));
      const echteVeilig    = (bestaand.veilig    || []).filter(p => p.offerId && !p.offerId.startsWith('test-'));

      if (echteProducten.length > 0) {
        testProducten = echteProducten.slice(0, 3);
        testVeilig    = echteVeilig.slice(0, 2);
        console.log(`✅ ${testProducten.length} echte producten + ${testVeilig.length} veilige geladen uit ${RAPPORT_PAD}`);
      } else {
        console.log(`⚠️  Geen echte offer IDs gevonden in ${RAPPORT_PAD} — nep-testartikelen gebruikt.`);
        console.log('   Draai éénmalig "node src/index.js" (zonder TEST_MODE) om echte offer IDs op te slaan.');
      }
    } catch {
      console.log(`⚠️  Geen ${RAPPORT_PAD} gevonden — nep-testartikelen gebruikt.`);
    }

    const nu = new Date();
    const uitvoer = {
      gegenereerd: nu.toISOString(),
      testModus: true,
      land: COUNTRY,
      aantalProducten: testProducten.length,
      aantalVeilig: testVeilig.length,
      producten: testProducten,
      veilig: testVeilig,
    };
    await writeFile(RAPPORT_PAD, JSON.stringify(uitvoer, null, 2), 'utf-8');
    console.log(`✅ Testrapport gegenereerd: ${testProducten.length} producten met concurrent, ${testVeilig.length} veilig`);
    console.log(`📄 ${RAPPORT_PAD} klaar — open het dashboard via: npm run serve`);
    return;
  }

  if (testEan) console.log(`🧪 Testmodus: alleen EAN ${testEan}\n`);

  const token = await getAccessToken();
  console.log('✅ Ingelogd bij bol.com API');
  await laadNaamCache();

  const processStatusId = await getEigenaanbiedingen(token);
  const csv = await wachtOpExport(token, processStatusId);
  let aanbiedingen = parseCsv(csv);
  if (aanbiedingen.length > 0) {
    const kolommen = Object.keys(aanbiedingen[0]);
    console.log(`📋 CSV-kolommen: ${kolommen.join(', ')}`);
    const heeftOfferId = kolommen.some(k => k.toLowerCase().includes('offer') && k.toLowerCase().includes('id'));
    if (!heeftOfferId) console.warn(`⚠️  Geen offerId-kolom gevonden — prijsaanpassing via dashboard werkt niet.`);

    // Filter op land als de CSV een countryCode-kolom bevat
    const landKolom = kolommen.find(k => /country|countrycode|land/i.test(k));
    if (landKolom) {
      const voor = aanbiedingen.length;
      aanbiedingen = aanbiedingen.filter(a => (a[landKolom] || '').toUpperCase() === COUNTRY);
      console.log(`🌍 Land gefilterd op ${COUNTRY}: ${aanbiedingen.length} van ${voor} aanbiedingen`);
    } else {
      console.log(`⚠️  Geen landkolom in CSV — alle aanbiedingen verwerkt (mix van NL/BE mogelijk)`);
    }
  }
  if (testEan) aanbiedingen = aanbiedingen.filter(a => a['ean'] === testEan);
  console.log(`📦 ${aanbiedingen.length} aanbiedingen — concurrent-prijzen ophalen...`);

  const kandidaten = [];
  const veiligeLijst = [];

  // Fase 1: concurrent-prijzen ophalen voor alle producten
  for (const aanbieding of aanbiedingen) {
    const ean = aanbieding['ean'];
    const eigenPrijs = parseFloat(aanbieding['bundlePricesPrice']);
    const referentie = aanbieding['referenceCode'] || ean;

    const offerIdKey = Object.keys(aanbieding).find(k => k.toLowerCase().replace(/[-_\s]/g, '') === 'offerid');
    const offerId = offerIdKey ? aanbieding[offerIdKey] : '';

    if (!ean || isNaN(eigenPrijs)) continue;

    const concurrentData = await getConcurrenten(token, ean);
    const offers = concurrentData.offers || [];
    const goedkopere = offers.filter(o => o.price && o.price < eigenPrijs && !o.bestOffer);

    if (goedkopere.length > 0) {
      kandidaten.push({ ean, offerId, eigenPrijs, referentie, goedkopere });
    } else {
      // Dichtstbijzijnde concurrent (goedkoopste aanbieder boven eigen prijs)
      const andereOffers = offers.filter(o => !o.bestOffer && o.price);
      const dichtstbij = andereOffers.length > 0 ? Math.min(...andereOffers.map(o => o.price)) : null;
      veiligeLijst.push({
        product: referentie,
        ean,
        offerId,
        eigenPrijs,
        dichtstbijzijneConcurrent: dichtstbij,
        marge: dichtstbij !== null ? parseFloat((dichtstbij - eigenPrijs).toFixed(2)) : null,
      });
    }
  }

  const nieuwInCache = () => Object.keys(naamCache).length;
  const cacheVoor = nieuwInCache();
  console.log(`💡 ${kandidaten.length} product(en) goedkoper, ${veiligeLijst.length} veilig — namen ophalen...`);

  // Fase 2: namen ophalen voor alle producten (gecached na eerste run)
  for (const v of veiligeLijst) {
    v.product = await getProductNaam(token, v.ean, v.product);
  }

  const rapport = [];
  for (const { ean, offerId, eigenPrijs, referentie, goedkopere } of kandidaten) {
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
      offerId,
      eigenPrijs,
      laagsteConcurrent: laagste,
      verschil: parseFloat((eigenPrijs - laagste).toFixed(2)),
      concurrenten: concurrentenMetNaam,
    });
  }

  rapport.sort((a, b) => b.verschil - a.verschil);

  // Naam-cache opslaan (nieuwe namen bijgehouden voor volgende run)
  const nieuweNamen = Object.keys(naamCache).length - cacheVoor;
  if (nieuweNamen > 0) console.log(`💾 ${nieuweNamen} nieuwe namen gecached`);
  await writeFile(NAAM_CACHE_PAD, JSON.stringify(naamCache), 'utf-8');

  const nu = new Date();
  const datumLabel = nu.toISOString().slice(0, 10); // bijv. 2026-04-11

  // JSON rapport (overschrijven)
  const uitvoer = {
    gegenereerd: nu.toISOString(),
    land: COUNTRY,
    aantalProducten: rapport.length,
    aantalVeilig: veiligeLijst.length,
    producten: rapport,
    veilig: veiligeLijst,
  };
  await writeFile(RAPPORT_PAD, JSON.stringify(uitvoer, null, 2), 'utf-8');

  // Excel rapport met datum en land in bestandsnaam
  await mkdir('rapporten', { recursive: true });
  const excelPad = `rapporten/prijsrapport-${COUNTRY}-${datumLabel}.xlsx`;

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
  console.log(`📄 JSON:  ${RAPPORT_PAD}`);
}

main().catch(console.error);
