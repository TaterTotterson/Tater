import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
const {chromium}=await import(process.env.TATER_PLAYWRIGHT_MODULE||'playwright');
const base=process.env.TATER_WORKSPACE_PREVIEW_URL||'http://127.0.0.1:5179/test-fixtures/core-workspaces.html';
const shots=process.env.TATER_WORKSPACE_SCREENSHOTS;
if(shots)await mkdir(shots,{recursive:true});
const browser=await chromium.launch({headless:true,...(process.env.CHROME_PATH?{executablePath:process.env.CHROME_PATH}:{})});
const page=await browser.newPage({viewport:{width:1440,height:1000}});
const errors=[];
page.on('pageerror',error=>errors.push(error.message));
page.on('console',message=>{if(message.text().includes('[Vue warn]'))errors.push(message.text());});
async function open(core){await page.goto(`${base}?core=${core}`);await page.locator('.cw-cores').waitFor();}
async function screenshot(name){if(shots)await page.screenshot({path:`${shots}/${name}.png`,fullPage:true});}
const layouts={ai_task_core:'tasks',automation_core:'rules',awareness_core:'events',environment_core:'weather',guardian_core:'network',memory_core:'memories',meshtastic_core:'radio',personal_core:'agenda',reachy_core:'behavior',rss_core:'feeds',tater_tube_core:'media',music_core:'music'};
try{
 for(const [core,layout] of Object.entries(layouts)){
  await open(core);
  await page.locator(`[data-workspace-layout="${layout}"]`).waitFor();
  for(const width of [1440,760,390]){
   await page.setViewportSize({width,height:1000});
   assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),`${core} overflows at ${width}px`);
  }
  await page.setViewportSize({width:1440,height:1000});
  if(['environment_core','awareness_core','personal_core','music_core'].includes(core))await screenshot(core);
  console.log(`PASS ${core}: ${layout} workspace at desktop, tablet, and mobile widths`);
 }
 await open('environment_core');
 await page.evaluate(()=>window.workspaceFixtures.environment_core.ui.item_forms=[]);
 await page.locator('.cw-weather-now').waitFor();
 assert.ok((await page.locator('.cw-weather-now').innerText()).includes('68°F'));
 assert.ok((await page.locator('.cw-purpose-context').innerText()).includes('Weather provider + local sensors'));
 console.log('PASS overview statistics remain visible without item cards');

 for(const core of ['environment_core','awareness_core','music_core']){
  await open(core);
  await page.evaluate(()=>document.documentElement.dataset.theme='tater');
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  const tab=page.locator('.core-manager-tab-btn.active,.tm-tab.active').first();
  if(await tab.count())assert.equal(await tab.evaluate(element=>getComputedStyle(element).backgroundImage),'none');
  await screenshot(`${core}-dark`);
 }
 console.log('PASS dark theme keeps the workspace layouts and flat navigation');

 await open('music_core');
 await page.evaluate(()=>window.workspaceMusic.payload.ui.item_forms.find(item=>item.group==='player').track_list=[
  {id:'past',title:'Already played'}, {id:'now',title:'Now playing',active:true}, {id:'next',title:'Next track'},
 ]);
 await page.locator('.tm-listening-context').getByText('Next track',{exact:true}).waitFor();
 assert.deepEqual(await page.locator('.tm-listening-context .tm-home-track strong').allTextContents(),['Next track']);
 console.log('PASS Up next follows the currently playing queue entry');
 await page.evaluate(()=>window.workspaceMusic.payload.ui.item_forms.find(item=>item.group==='player').popup_fields=[{key:'playback_note',label:'Playback note',type:'text',value:'Preserved setting'}]);
 await page.getByRole('button',{name:'Choose speakers and players',exact:true}).click();
 assert.equal(await page.getByLabel('Playback note',{exact:true}).inputValue(),'Preserved setting');
 await page.getByLabel('Playback note',{exact:true}).fill('Edited setting');
 await page.getByRole('button',{name:'Reset changes',exact:true}).click();
 assert.equal(await page.getByLabel('Playback note',{exact:true}).inputValue(),'Preserved setting');
 console.log('PASS Music speaker settings remain editable and can be reset');

 await open('ai_task_core');
 await page.getByRole('button',{name:'Edit task',exact:true}).click();
 await page.getByLabel('Task title',{exact:true}).fill('Edited briefing');
 await page.getByRole('button',{name:'Schedule',exact:true}).click();
 await page.getByRole('textbox',{name:'Schedule',exact:true}).fill('Every weekday at 8:00 AM');
 // An SSE-style refresh must not overwrite newer edits in any group.
 await page.evaluate(()=>window.workspaceFixtures.ai_task_core.ui.item_forms[0].fields.find(field=>field.key==='title').value='Server title');
 await page.getByRole('button',{name:'Save',exact:true}).click();
 const saved=await page.evaluate(()=>window.workspaceRequests.find(request=>request.action==='save_item'));
 assert.equal(saved.payload.values.title,'Edited briefing');
 assert.equal(saved.payload.values.schedule_text,'Every weekday at 8:00 AM');
 assert.equal(saved.payload.values.future_option,'Preserved');
 assert.equal(saved.payload.id,'morning');
 await page.getByRole('button',{name:'Back to details',exact:true}).click();
 await page.getByRole('button',{name:'Run Now',exact:true}).click();
 assert.ok(await page.evaluate(()=>window.workspaceRequests.some(request=>request.action==='run_item'&&request.payload.id==='morning')));
 console.log('PASS task edit, grouped fields, refresh-safe drafts, save payload, and Run Now');

 await open('automation_core');
 await page.getByRole('button',{name:'Create automation',exact:true}).click();
 await page.locator('.cw-flow-editor').waitFor();
 for(const heading of ['When this happens','Optional conditions','Then do this','Voice & background audio'])await page.getByRole('heading',{name:heading,exact:true}).waitFor();
 await screenshot('automation_flow');
 console.log('PASS automation editor exposes the complete trigger/condition/action/audio flow');

 await open('meshtastic_core');
 await page.getByPlaceholder('Message #LongFast…').fill('Radio test');
 await page.getByPlaceholder('Message #LongFast…').press('Enter');
 assert.ok(await page.evaluate(()=>window.workspaceRequests.some(request=>request.action==='send_message'&&request.payload.values.text==='Radio test'&&request.payload.values.channel==='0')));
 console.log('PASS radio Enter-to-send preserves the channel and message');

 await open('rss_core');
 assert.ok(!(await page.locator('.cw-item-preview').innerText()).includes('never-preview-this'));
 await page.getByRole('button',{name:'Edit feed',exact:true}).click();
 await page.getByRole('button',{name:'Connection options',exact:true}).click();
 await page.getByLabel('Notification token',{exact:true}).waitFor();
 console.log('PASS credentials stay out of summaries and remain available in the editor');
 assert.deepEqual(errors,[]);
 console.log('PASS no browser runtime errors');
}finally{await browser.close();}
