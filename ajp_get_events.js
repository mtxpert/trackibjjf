const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const allIds = new Set();
  
  // Check how many pages
  await page.goto('https://ajptour.com/en/events/past', { waitUntil: 'networkidle', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(2000);
  
  // Get total pages
  const pageCount = await page.evaluate(() => {
    const links = [...document.querySelectorAll('a[href]')];
    const pageNums = links.map(a => {
      const m = a.href.match(/page=(\d+)/);
      return m ? parseInt(m[1]) : 0;
    });
    return Math.max(...pageNums, 1);
  });
  console.log('Total pages:', pageCount);
  
  for (let pg = 1; pg <= Math.min(pageCount, 20); pg++) {
    const url = pg === 1 ? 'https://ajptour.com/en/events/past' : `https://ajptour.com/en/events/past?page=${pg}`;
    if (pg > 1) await page.goto(url, { waitUntil: 'networkidle', timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1500);
    
    const ids = await page.evaluate(() => {
      const ids = new Set();
      document.querySelectorAll('a[href]').forEach(a => {
        const m = a.href.match(/\/event\/(\d+)/);
        if (m) ids.add(m[1]);
      });
      if (window.sc && window.sc.events) window.sc.events.forEach(e => ids.add(String(e.id)));
      return [...ids];
    });
    ids.forEach(id => allIds.add(id));
    process.stdout.write(`Page ${pg}/${pageCount}: ${ids.length} ids (total ${allIds.size})\n`);
  }
  
  console.log('\nAll AJP event IDs:', [...allIds].sort((a,b)=>a-b).join(','));
  console.log('Total:', allIds.size);
  await browser.close();
})();
