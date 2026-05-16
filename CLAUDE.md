# Bol.com Prijsmonitor — projectoverzicht voor Claude

## Wat doet dit project

Dagelijkse prijsmonitor voor een bol.com verkoper. Vergelijkt eigen prijzen met concurrenten en toont die in een dashboard. Prijzen kunnen vanuit het dashboard direct op bol.com worden aangepast.

Ondersteunde landen: **NL** en **BE** (apart per land, eigen API-credentials).

## Bestanden

| Bestand | Doel |
|---|---|
| `src/index.js` | Nachtelijke script: haalt aanbiedingen op, vergelijkt concurrenten, slaat rapport op |
| `src/prijs-aanpassen.js` | Voert daadwerkelijke prijswijzigingen uit op bol.com via API |
| `server.js` | Lokale dev-server (statische bestanden + API-proxy voor lokaal testen) |
| `index.html` | Dashboard (pure HTML/JS, geen framework) |
| `rapport-NL.json` / `rapport-BE.json` | Gegenereerde rapporten, gecommit door GitHub Actions |
| `naam-cache-NL.json` / `naam-cache-BE.json` | Cache van productnamen (EAN → naam), TTL 7 dagen |
| `retailer-cache-NL.json` / `retailer-cache-BE.json` | Cache van retailernamen (retailerId → naam), persistent |
| `.github/workflows/dagelijks-rapport.yml` | Nachtelijke GitHub Actions run |
| `.github/workflows/prijs-aanpassen.yml` | GitHub Actions workflow voor prijswijzigingen (gestart vanuit dashboard) |

## Dataflow

```
Nachtelijke run (src/index.js):
  bol.com API → eigen aanbiedingen (CSV export)
             → concurrent-aanbiedingen per EAN
             → productnamen (naam-cache, 7 dagen TTL)
             → retailernamen (retailer-cache, persistent)
  → rapport-{land}.json  (gecommit)
  → naam-cache-{land}.json  (gecommit)
  → retailer-cache-{land}.json  (gecommit)

Dashboard (index.html):
  Laadt rapport-{land}.json → toont tabel
  Laadt naam-cache + retailer-cache → verrijkt namen
  Gebruiker past prijs aan → opslaan via:
    Lokaal: POST /api/prijs-aanpassen → src/prijs-aanpassen.js
    Productie: GitHub Actions workflow prijs-aanpassen.yml
```

## Omgevingsvariabelen (.env)

```
BOL_CLIENT_ID_NL=...
BOL_CLIENT_SECRET_NL=...
BOL_CLIENT_ID_BE=...
BOL_CLIENT_SECRET_BE=...
GITHUB_TOKEN=...      # voor dashboard → GitHub Actions koppeling
REPO=Ylona/bol-prijsmonitor
```

## Lokaal draaien

```bash
npm run serve          # start dev-server op :3000
npm run test-mode      # genereer testrapport op basis van bestaand rapport
npm run test:ean       # test met één specifiek EAN
```

## Architectuurkeuzes

- **Geen framework**: dashboard is plain HTML/JS. Aanpassingen = directe edits in index.html.
- **Twee-fasen namen**: rapport slaat namen op bij generatie; dashboard verrijkt daarna via cache-JSON. Zo zijn namen zichtbaar ook als de cache-lookup tijdens de nachtelijke run faalde.
- **Persistente retailer-cache**: retailernamen worden opgeslagen in `retailer-cache-{land}.json` omdat de bol.com API soms retailernamen niet teruggeeft (dan staat alleen het numerieke ID in het rapport).
- **GitHub Pages**: het dashboard wordt geserveerd via GitHub Pages direct uit de `main` branch root.
- **Land-scheiding**: NL en BE hebben volledig gescheiden credentials, rapporten en caches.

## Veelgemaakte wijzigingen

### Namen werken niet in dashboard
1. Controleer of `naam-cache-{land}.json` en `retailer-cache-{land}.json` bestaan en gevuld zijn.
2. De knop "↺ Namen bijwerken" in het dashboard herlaadt beide caches.
3. Als caches leeg zijn: draai de nachtelijke script handmatig via GitHub Actions.

### Nieuwe kolom toevoegen aan rapport
1. Voeg veld toe in `src/index.js` bij de objecten in `rapport.push(...)` en `veiligeLijst.push(...)`.
2. Voeg het toe aan het Excel-exportblad in `src/index.js` bij `blad.columns`.
3. Toon het in `index.html` in de `render()` en `renderVeilig()` functies.

### Prijswijziging flow (productie)
1. Gebruiker klikt "Opslaan op bol.com" → modal toont wijzigingen
2. Bevestigen → POST naar GitHub API om `prijs-aanpassen.yml` te starten
3. Dashboard pollt workflow-status elke 8s, max 40 pogingen (~5 min)
4. Bij succes: `verwerkOpslaanSucces()` past `eigenPrijs` aan in geheugen

## Bekende beperkingen

- De CSV-parser in `parseCsv()` is naïef (split op komma). Werkt voor bol.com's CSV-format maar breekt als velden komma's bevatten.
- Retailernamen die tijdens de nachtelijke run niet ophaalbaar zijn (API-fout) blijven als numeriek ID in rapport tot de retailer-cache ze heeft.
- `pollPrijsUpdate` gebruikt de meest recente workflow run, niet specifiek de run die net gestart is. Als er gelijktijdig een andere run actief is, kan de status verwarrend zijn.
