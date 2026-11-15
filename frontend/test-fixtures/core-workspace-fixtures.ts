import defaults from '../src/cores/workspaces.json';
import type {JsonRow} from '../src/cores/types';

// Synthetic fixtures for the real workspace components. No live service is used.
const field=(key:string,label:string,value:any,type='text',extra:JsonRow={})=>({key,label,value,type,...extra});
const table=(key:string,label:string,columns:string[],rows:any[])=>field(key,label,'','table',{columns,rows});
const item=(id:string,group:string,title:string,fields:JsonRow[]=[],extra:JsonRow={})=>({id,group,title,fields,...extra});
const tab=(key:string,label:string,extra:JsonRow={})=>({key,label,source:'items',item_group:key,...extra});
const editFields=[field('name','Name','Sample item'),field('enabled','Enabled',true,'checkbox'),field('future_option','Future option','Preserved')];
const actionItem=(id:string,group:string,title:string,extra:JsonRow={})=>item(id,group,title,editFields,{subtitle:'A saved item from the test fixture',save_action:'save_item',run_action:'run_item',remove_action:'remove_item',...extra});
const payload=(key:string,tabs:JsonRow[],items:JsonRow[],extra:JsonRow={})=>({summary:'Workspace test data',stats:[{label:'Items',value:items.length}],ui:{kind:'settings_manager',workspace:(defaults as JsonRow)[key],manager_tabs:tabs,item_forms:items,...extra}});
export const fixtures:Record<string,JsonRow>={
  ai_task_core:payload('ai_task_core',[tab('items','Tasks'),tab('create','Create task',{source:'add_form'})],[
    actionItem('morning','items','Morning briefing',{fields:[field('title','Task title','Morning briefing'),field('task_prompt','What should Tater do?','Weather, the day ahead, and a positive message.','textarea'),field('schedule_text','Schedule','Every day at 7:00 AM'),field('destination','Destination','Kitchen'),...editFields]}),
    actionItem('friday','items','Friday check-in'),
  ],{add_form:{action:'create_item',submit_label:'Save task',fields:editFields}}),
  automation_core:payload('automation_core',[tab('rules','Automations'),tab('create','Create automation',{source:'add_form'}),tab('history','Run history')],[
    actionItem('driveway','rules','Driveway announcement',{fields:[...editFields,field('trigger','Trigger','Person detected · Driveway','text',{read_only:true}),field('action_type','Action','announce','select',{options:[{value:'announce',label:'Make an announcement'}]}),field('last_status','Latest result','Delivered','text',{read_only:true})]}),
    item('run-1','history','Driveway announcement',[field('result','Result','Announcement delivered','textarea',{read_only:true})],{detail:'Today · 10:18 AM'}),
  ],{add_form:{action:'create_item',submit_label:'Save automation',fields:[...editFields,field('trigger_device','Device','driveway','select',{options:[{value:'driveway',label:'Driveway camera'}]}),field('trigger_event','Event','Person detected'),field('trigger_person_id','Person match','Anyone'),field('action_type','Action','Make an announcement'),field('tts_text','Announcement','Someone is at the driveway.','textarea'),field('tts_background_volume_percent','Background volume',20,'number')]}}),
  awareness_core:payload('awareness_core',[tab('events','Event history'),tab('monitors','Monitored sources'),tab('add','Add source',{source:'add_form'})],[
    item('event-1','events','Front door',[field('description','Description','A person approached the entrance.','textarea',{read_only:true}),field('snapshot','Snapshot','','image',{src:'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="500" height="240"%3E%3Crect width="500" height="240" fill="%23545b4d"/%3E%3C/svg%3E'})],{subtitle:'Today · 10:18 AM · Person detected'}),
    item('event-2','events','Patio door',[field('description','Description','The patio door opened.','textarea',{read_only:true})],{subtitle:'Today · 9:12 AM · Sensor event'}),
    actionItem('source-1','monitors','Front door camera'),
  ],{add_form:{action:'create_item',fields:editFields}}),
  environment_core:{...payload('environment_core',[tab('overview','Overview'),tab('forecast','Forecast'),tab('trends','Tater Trends'),tab('sources','Sources')],[
    item('conditions','overview','Local readings',[table('environment_area_overview','Areas',['Area','Temperature','Humidity'],[{Area:'Living room',Temperature:'72°F',Humidity:'43%'},{Area:'Kitchen',Temperature:'71°F',Humidity:'45%'}])]),
    item('forecast','forecast','Daily forecast',[table('weatherapi_daily_forecast','Daily forecast',['Day','Conditions','High','Low'],[{Day:'Today',Conditions:'Partly cloudy',High:'72°F',Low:'51°F'},{Day:'Tomorrow',Conditions:'Sunny',High:'74°F',Low:'53°F'}])]),
    item('trend','trends','Recent conditions',[table('history','Reading history',['Time','Temperature'],[{Time:'Now',Temperature:'72°F'}])]),
    actionItem('source-1','sources','Living room'),
  ]),stats:[{label:'Outdoor Temp',value:'68°F'},{label:'Forecast',value:'Partly cloudy'},{label:'Humidity',value:'58%'},{label:'Wind',value:'6 mph'},{label:'Status',value:'Live'},{label:'Sources',value:'Weather provider + local sensors'},{label:'Last Sample',value:'Just now'}]},
  guardian_core:payload('guardian_core',[tab('ai_overview','AI overview'),tab('confirm','Confirm'),tab('devices','Devices'),tab('events','Events')],[
    item('brief','ai_overview','Network briefing',[field('guardian_ai_overview_text','What needs your attention','One new device needs a friendly label.','textarea',{read_only:true}),table('guardian_ai_overview_signals','Signals',['Device','State'],[{Device:'Home router',State:'Reachable'}])]),
    actionItem('device-1','confirm','Unlabeled device'),actionItem('device-2','devices','Living room speaker'),item('change-1','events','Speaker returned online',[],{subtitle:'Today · 9:08 AM'}),
  ]),
  memory_core:payload('memory_core',[tab('knows','Knows'),tab('overview','Overview & insights'),tab('people','People'),tab('tools','Tools')],[
    actionItem('memory-1','knows','A remembered preference',{fields:[field('preview_text','Memory','Prefers short, practical answers.','textarea',{read_only:true}),...editFields]}),
    item('insight','overview','Useful context',[field('overview_text','Memory insights','Preferences help Tater respond naturally.','textarea',{read_only:true})]),
    actionItem('person-1','people','Casey'),actionItem('tool','tools','Memory tools'),
  ]),
  meshtastic_core:{summary:'Your connected mesh radio',stats:[],ui:{kind:'channel_chat',workspace:defaults.meshtastic_core,status:{connected:true,label:'Connected',radio_name:'Home radio',transport:'Echo satellite Bluetooth'},channels:[{id:'0',label:'LongFast',subtitle:'Broadcast channel'},{id:'1',label:'Local'}],default_channel:'0',messages:[{id:'message-1',channel:'0',direction:'inbound',sender_name:'Trail relay',text:'Signal is good from this end.',timestamp:'2026-11-15T10:12:00Z'}],composer:{action:'send_message',max_length:200},manager_tabs:[tab('nodes','Nodes'),tab('bluetooth','Bluetooth pairing')],item_forms:[actionItem('node-1','nodes','Home radio'),actionItem('radio-1','bluetooth','Nearby radio')]}},
  personal_core:payload('personal_core',[tab('calendar','Agenda'),tab('overview','Overview'),tab('accounts','Accounts')],[
    item('today','calendar','Today',[table('calendar_today_table','Today',['when','title','account'],[{when:'9:30 AM',title:'Team check-in',account:'Work calendar'},{when:'4:00 PM',title:'Take a walk',account:'Personal calendar'}])]),
    item('overview','overview','Plans & deliveries',[field('summary_text','Your day','Two calendar events and one package delivery.','textarea',{read_only:true})]),actionItem('account-1','accounts','Personal account'),
  ]),
  reachy_core:payload('reachy_core',[{key:'reachy-1',label:'Desk Reachy',source:'grouped_items',groups:[{key:'motion',label:'Motion & Music',item_group:'motion',selector:false},{key:'watch',label:'Tracking',item_group:'watch',selector:false}]}],[
    actionItem('motion-1','motion','Motion & Music',{subtitle:'Desk Reachy · Living room',fields:[field('enabled','Natural motion',true,'checkbox'),field('strength','Motion strength',60,'range',{min:0,max:100}),...editFields]}),actionItem('tracking-1','watch','Tracking'),
  ]),
  rss_core:payload('rss_core',[tab('feeds','Feeds'),tab('create','Add feed',{source:'add_form'})],[actionItem('feed-1','feeds','Daily reading',{fields:[field('url','Feed URL','https://example.test/feed.xml'),...editFields,field('portals.ntfy.token','Notification token','never-preview-this','password')]}),actionItem('feed-2','feeds','Technology notes')],{add_form:{action:'create_item',fields:editFields}}),
  tater_tube_core:payload('tater_tube_core',[tab('overview','Overview'),tab('picks',"Tater’s Picks"),tab('history','Activity context')],[
    item('connection','overview','Home media',[],{subtitle:'Connected',detail:'Tater Tube Server',run_action:'sync_now',run_label:'Sync now'}),item('pick-1','picks','A quieter world',[],{subtitle:'A documentary to wind down with'}),item('pick-2','picks','Small discoveries',[],{subtitle:'A short watch with a little curiosity'}),item('watch-1','history','Evening viewing',[],{subtitle:'Living room · A short documentary'}),
  ]),
};

export const musicFixture:JsonRow={summary:'Music library',stats:[{label:'Albums',value:2}],ui:{appearance:'music_library',workspace:defaults.music_core,manager_tabs:[{key:'library',label:'Library',source:'grouped_items',groups:[{key:'albums',label:'Albums',item_group:'albums'}]},{key:'playlist',label:'Playlist',source:'player_queue'},{key:'recommendations',label:'Recommendations',source:'items',item_group:'recommendations'}],item_forms:[{id:'album-1',group:'albums',title:'Stillwater',subtitle:'Sunroom',run_action:'play_album',run_label:'Play'},{id:'album-2',group:'albums',title:'Paper Lanterns',subtitle:'Open windows',run_action:'play_album',run_label:'Play'},{id:'player',group:'player',title:'Sunroom',subtitle:'Stillwater · Kitchen',track_list:[{id:'track-1',title:'Sunroom',artist:'Stillwater',active:true},{id:'track-2',title:'Open windows',artist:'Paper Lanterns'}],actions:[{action:'pause',label:'Pause'}],fields:[],popup_fields:[]}]}};
