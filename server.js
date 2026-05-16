import { createServer } from 'http';
import { readFile, writeFile } from 'fs/promises';
import { extname, join } from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const PORT = process.env.PORT || 3000;

const MIMES = {
  '.html': 'text/html; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.js':   'text/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.ico':  'image/x-icon',
};

function leesBody(req) {
  return new Promise(resolve => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => resolve(body));
  });
}

createServer(async (req, res) => {
  // API: pas prijzen aan op bol.com via lokale .env credentials
  if (req.method === 'POST' && req.url === '/api/prijs-aanpassen') {
    const body = await leesBody(req);
    let wijzigingen, land;
    try {
      const parsed = JSON.parse(body);
      wijzigingen = parsed.wijzigingen;
      land = (parsed.land || 'NL').toUpperCase();
    } catch {
      wijzigingen = [];
      land = 'NL';
    }
    console.log(`🔧 Prijzen aanpassen (${land}): ${wijzigingen.length} product(en)...`);

    const child = spawn('node', ['src/prijs-aanpassen.js'], {
      env: { ...process.env, WIJZIGINGEN: JSON.stringify(wijzigingen), COUNTRY: land },
      cwd: __dirname,
    });
    child.stdout.on('data', d => process.stdout.write(d));
    child.stderr.on('data', d => process.stderr.write(d));
    let gestuurd = false;
    child.on('close', code => {
      if (gestuurd) return;
      gestuurd = true;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: code === 0 }));
    });
    child.on('error', err => {
      if (gestuurd) return;
      gestuurd = true;
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, fout: err.message }));
    });
    return;
  }

  // API: schrijf nieuwe eigenPrijs terug naar rapport-{land}.json na succesvolle opslaan
  if (req.method === 'POST' && req.url === '/api/patch-rapport') {
    const body = await leesBody(req);
    try {
      const parsed = JSON.parse(body);
      const wijzigingen = parsed.wijzigingen || [];
      const land = (parsed.land || 'NL').toUpperCase();
      const pad = join(__dirname, `rapport-${land}.json`);
      const rapport = JSON.parse(await readFile(pad, 'utf-8'));
      for (const w of wijzigingen) {
        const p = (rapport.producten || []).find(x => x.ean === w.ean);
        if (p) p.eigenPrijs = w.prijs;
        const v = (rapport.veilig || []).find(x => x.ean === w.ean);
        if (v) v.eigenPrijs = w.prijs;
      }
      await writeFile(pad, JSON.stringify(rapport, null, 2), 'utf-8');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, fout: err.message }));
    }
    return;
  }

  // API: genereer testdata opnieuw
  if (req.method === 'POST' && req.url === '/api/vernieuwen') {
    const body = await leesBody(req);
    let land = 'NL';
    try { land = (JSON.parse(body).land || 'NL').toUpperCase(); } catch {}
    console.log(`🔄 Testdata vernieuwen (${land})...`);
    const child = spawn('node', ['src/index.js'], {
      env: { ...process.env, TEST_MODE: 'true', COUNTRY: land },
      cwd: __dirname,
    });
    child.stdout.on('data', d => process.stdout.write(d));
    child.stderr.on('data', d => process.stderr.write(d));
    let gestuurd = false;
    child.on('close', code => {
      if (gestuurd) return;
      gestuurd = true;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: code === 0 }));
    });
    child.on('error', err => {
      if (gestuurd) return;
      gestuurd = true;
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, fout: err.message }));
    });
    return;
  }

  // Statische bestanden serveren
  const padDeel = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  const bestand = join(__dirname, decodeURIComponent(padDeel));

  try {
    const inhoud = await readFile(bestand);
    const mime = MIMES[extname(bestand)] || 'text/plain';
    res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'no-cache' });
    res.end(inhoud);
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('404 Niet gevonden');
  }
}).listen(PORT, () => {
  console.log(`\n🌐 Dashboard klaar op  http://localhost:${PORT}`);
  console.log('   Druk op Ctrl+C om te stoppen.\n');
});
