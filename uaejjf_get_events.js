const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  // Intercept getResults calls to find which events have data
  const apiCalls = [];
  page.on('response', async resp => {
    const url = resp.url();
    if (url.includes('/results/getResults')) {
      const m = url.match(/\/event\/(\d+)\//);
      if (m) {
        try {
          const body = await resp.json();
          const count = (body.eventResults || []).length;
          apiCalls.push({ id: m[1], categories: count });
        } catch(e) {}
      }
    }
  });

  const allEventIds = new Set();
  
  for (let pg = 1; pg <= 6; pg++) {
    const url = pg === 1 
      ? 'https://events.uaejjf.org/en/events/past'
      : `https://events.uaejjf.org/en/events/past?page=${pg}`;
    console.log(`Loading page ${pg}: ${url}`);
    await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(2000);
    
    // Extract event IDs from links and window.sc
    const ids = await page.evaluate(() => {
      const ids = new Set();
      document.querySelectorAll('a[href]').forEach(a => {
        const m = a.href.match(/\/event\/(\d+)/);
        if (m) ids.add(m[1]);
      });
      // Also from window.sc if it has events array
      if (window.sc && window.sc.events) {
        window.sc.events.forEach(e => ids.add(String(e.id)));
      }
      return [...ids];
    });
    
    ids.forEach(id => allEventIds.add(id));
    console.log(`  Page ${pg}: found IDs: ${ids.join(', ')}`);
  }
  
  console.log('\nAll event IDs:', [...allEventIds].sort((a,b) => a-b).join(', '));
  console.log('Total:', allEventIds.size);
  
  await browser.close();
})();
