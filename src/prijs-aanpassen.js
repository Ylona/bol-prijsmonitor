import fetch from 'node-fetch';
import * as dotenv from 'dotenv';
dotenv.config();

const CLIENT_ID  = process.env.BOL_CLIENT_ID;
const CLIENT_SECRET = process.env.BOL_CLIENT_SECRET;
const WIJZIGINGEN = JSON.parse(process.env.WIJZIGINGEN || '[]');

let lastCallTime = 0;
async function bolFetch(url, options = {}) {
  const wacht = Math.max(0, 200 - (Date.now() - lastCallTime));
  if (wacht > 0) await new Promise(r => setTimeout(r, wacht));
  lastCallTime = Date.now();
  const res = await fetch(url, options);
  if (res.status === 429) {
    console.log('⏸ Rate limit — 3s wachten...');
    await new Promise(r => setTimeout(r, 3000));
    return bolFetch(url, options);
  }
  return res;
}

async function getAccessToken() {
  const creds = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
  const res = await fetch('https://login.bol.com/token?grant_type=client_credentials', {
    method: 'POST',
    headers: { 'Authorization': `Basic ${creds}`, 'Accept': 'application/json' },
  });
  if (!res.ok) throw new Error(`Token ophalen mislukt (${res.status}): ${await res.text()}`);
  const data = await res.json();
  if (!data.access_token) throw new Error('Geen access_token ontvangen');
  return data.access_token;
}

async function getAanbieding(token, offerId) {
  const res = await bolFetch(`https://api.bol.com/retailer/offers/${offerId}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
    },
  });
  if (!res.ok) throw new Error(`Aanbieding ophalen mislukt (${res.status}): ${await res.text()}`);
  return res.json();
}

async function updatePrijs(token, offerId, offer, nieuwePrijs) {
  // Behoud alle bestaande bundelprijzen, verander alleen de basisprijs (quantity 1)
  const bundlePrices = (offer.pricing?.bundlePrices || []).map(bp =>
    bp.quantity === 1
      ? { quantity: bp.quantity, unitPrice: nieuwePrijs }
      : { quantity: bp.quantity, unitPrice: bp.unitPrice }
  );
  if (!bundlePrices.some(bp => bp.quantity === 1)) {
    bundlePrices.unshift({ quantity: 1, unitPrice: nieuwePrijs });
  }

  // Prijzen worden via een apart endpoint bijgewerkt in bol.com API v10
  const res = await bolFetch(`https://api.bol.com/retailer/offers/${offerId}/price`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.retailer.v10+json',
      'Content-Type': 'application/vnd.retailer.v10+json',
    },
    body: JSON.stringify({ pricing: { bundlePrices } }),
  });

  if (!res.ok) {
    const tekst = await res.text();
    throw new Error(`Aanpassen mislukt (${res.status}): ${tekst}`);
  }
}

async function main() {
  if (!WIJZIGINGEN.length) {
    console.log('Geen wijzigingen opgegeven.');
    return;
  }

  console.log(`🔧 ${WIJZIGINGEN.length} prijs(wijziging(en)) aanpassen op bol.com...\n`);
  const token = await getAccessToken();
  console.log('✅ Ingelogd bij bol.com\n');

  let succes = 0;
  let mislukt = 0;

  for (const w of WIJZIGINGEN) {
    try {
      const offer = await getAanbieding(token, w.offerId);
      await updatePrijs(token, w.offerId, offer, w.prijs);
      console.log(`✅ ${w.product}: €${Number(w.oudePrijs).toFixed(2)} → €${Number(w.prijs).toFixed(2)}`);
      succes++;
    } catch (err) {
      console.error(`❌ ${w.product} (${w.offerId}): ${err.message}`);
      mislukt++;
    }
  }

  console.log(`\n📊 Klaar: ${succes} gelukt, ${mislukt} mislukt`);
  if (mislukt > 0) process.exit(1);
}

main().catch(err => {
  console.error('Fatale fout:', err.message);
  process.exit(1);
});
