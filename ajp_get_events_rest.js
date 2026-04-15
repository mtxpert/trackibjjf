const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const allIds = new Set();
  for (let pg = 21; pg <= 27; pg++) {
    await page.goto(`https://ajptour.com/en/events/past?page=${pg}`, { waitUntil: 'networkidle', timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1500);
    const ids = await page.evaluate(() => {
      const ids = new Set();
      document.querySelectorAll('a[href]').forEach(a => {
        const m = a.href.match(/\/event\/(\d+)/);
        if (m) ids.add(m[1]);
      });
      return [...ids];
    });
    ids.forEach(id => allIds.add(id));
    process.stdout.write(`Page ${pg}: ${ids.length} ids\n`);
  }
  console.log('Extra IDs pages 21-27:', [...allIds].sort((a,b)=>a-b).join(','));
  await browser.close();
})();
