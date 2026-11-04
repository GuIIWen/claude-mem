import { afterAll, afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { SessionStore } from '../../src/services/sqlite/SessionStore.js';
import { DeferredObserverQueue } from '../../src/services/worker/DeferredObserverQueue.js';
import { SessionManager } from '../../src/services/worker/SessionManager.js';
import type { DatabaseManager } from '../../src/services/worker/DatabaseManager.js';
import type { PendingMessage, ActiveSession } from '../../src/services/worker-types.js';
import { beginBatch, commitBatchOutput, derivedAttribution, freezeSource, makeBatchEntry, newBatchId,
  prepareEventFragments, quarantineBatch, renderBatch, retainBatchOutput, validateBatchOutput, type ObserverBatch } from '../../src/services/worker/observer-batch-v2.js';
import { processAgentResponse, sanitizeV2ObservationFiles } from '../../src/services/worker/agents/ResponseProcessor.js';
import { ModeManager } from '../../src/services/domain/ModeManager.js';

const modulePaths = ['@anthropic-ai/claude-agent-sdk', '../../src/shared/find-claude-executable.js',
  '../../src/shared/EnvManager.js', '../../src/supervisor/process-registry.js', '../../src/services/context-generator.js'] as const;
const actual = await Promise.all(modulePaths.map(async path => ({...await import(path)})));
const sends: Array<{session: number; batch: ObserverBatch; prompt: string}> = [];
let answer: (batch: ObserverBatch) => string;
let onAfterResult: (() => void) | undefined;
let sdkOutcome: 'success' | 'eof' | 'error' = 'success';
let currentManager: SessionManager;
let currentSession: ActiveSession;
mock.module(modulePaths[0], () => ({...actual[0], query: ({prompt}: {prompt: AsyncIterable<{message:{content:unknown}}>}) => (async function* () {
  yield {type:'system',subtype:'init',session_id:`sdk-${currentSession.sessionDbId}`};
  for await (const input of prompt) {
    const batch = currentSession.observerV2Batch!;
    sends.push({session:currentSession.sessionDbId,batch,prompt:String(input.message.content)});
    if (sdkOutcome === 'eof') return;
    const text = answer(batch);
    // Deliberately split the XML across assistant frames. Parsing must wait for the result boundary.
    const middle = Math.floor(text.length / 2);
    if (sdkOutcome !== 'error') for (const part of [text.slice(0,middle),text.slice(middle)]) yield {type:'assistant',message:{content:[{type:'text',text:part}],usage:{input_tokens:10,output_tokens:1}}};
    yield {type:'result',subtype:sdkOutcome==='error'?'error_during_execution':'success',is_error:sdkOutcome==='error',uuid:`request-${sends.length}`,usage:{input_tokens:100,output_tokens:20}};
    onAfterResult?.();
    if (currentManager.getMessageBuffer().getPendingCount(currentSession.sessionDbId) === 0) {
      currentSession.abortReason = 'idle'; currentSession.abortController.abort();
    }
  }
})()}));
mock.module(modulePaths[1], () => ({...actual[1],findClaudeExecutable:() => '/mock/claude'}));
mock.module(modulePaths[2], () => ({...actual[2],buildIsolatedEnvWithFreshOAuth:async() => ({PATH:process.env.PATH ?? ''}),getAuthMethodDescription:() => 'test'}));
mock.module(modulePaths[3], () => ({...actual[3],waitForSlot:async() => ({release:()=>{}}),createSdkSpawnFactory:() => () => {throw Error('no real spawn');},getSdkProcessForSession:()=>undefined}));
mock.module(modulePaths[4], () => ({...actual[4],generateContext:async() => ''}));
afterAll(() => modulePaths.forEach((path,index) => mock.module(path,() => actual[index])));
const { ClaudeProvider } = await import('../../src/services/worker/ClaudeProvider.js');

let store: SessionStore;
let manager: SessionManager;
let dbManager: DatabaseManager;
const chromaCalls: unknown[][]=[];
let sid: number;
const restoreEnv = new Map<string,string|undefined>();
beforeEach(() => {
  ModeManager.getInstance().loadMode('code');
  sends.length=0; chromaCalls.length=0; onAfterResult=undefined; sdkOutcome='success';
  for (const [key,value] of Object.entries({CLAUDE_MEM_CLAUDE_BATCH_V2_ENABLED:'true',CLAUDE_MEM_CLAUDE_BATCH_V2_MAX_CHARS:'96000',
    CLAUDE_MEM_OBSERVER_MAX_CONVERSATION_CHARS:'400000',CLAUDE_MEM_OBSERVER_CONTEXT_WINDOW:'200000',CLAUDE_MEM_FOLDER_CLAUDEMD_ENABLED:'false'})) {
    restoreEnv.set(key,process.env[key]); process.env[key]=value;
  }
  store=new SessionStore(':memory:');
  sid=store.createSDKSession('content-one','note','do work');
  store.updateMemorySessionId(sid,'memory-one');
  dbManager={getSessionById:(id:number)=>store.getSessionById(id),getSessionStore:()=>store,
    getChromaSync:()=>({syncObservation:async(...args:unknown[])=>{chromaCalls.push(args);},syncSummary:async()=>{}}),getCloudSync:()=>null} as unknown as DatabaseManager;
  manager=new SessionManager(dbManager);currentManager=manager;
  manager.configureDeferredObserver(store.db,{CLAUDE_MEM_OBSERVER_TRIGGER:'summary_or_session_end',CLAUDE_MEM_OBSERVER_MAX_WAIT_MS:'0'});
  answer=batch => batch.entries.map((entry,index)=>entry.message.type==='summarize'?summary([entry.label],`request-${entry.source.prompt_number}`):obs([entry.label],`fact-${entry.source.event_id}-${entry.fragmentIndex}`)).join('');
});
afterEach(async () => {
  await manager.shutdownAll(); store.close();
  for(const[key,value]of restoreEnv){if(value===undefined)delete process.env[key];else process.env[key]=value;}restoreEnv.clear();
});
function obs(labels:string[],title='A fact'):string{return `<observation><type>discovery</type><title>${title}</title><facts><fact>evidence</fact></facts><narrative>done</narrative><concepts></concepts><files_read></files_read><files_modified></files_modified>${sources(labels)}</observation>`;}
function summary(labels:string[],request='work'):string{return `<summary><request>${request}</request><investigated>source</investigated><learned>lesson</learned><completed>done</completed><next_steps>none</next_steps>${sources(labels)}</summary>`;}
function sources(labels:string[]):string{return `<source_event_ids>${labels.map(l=>`<source>${l}</source>`).join('')}</source_event_ids>`;}
function add(count:number,session=sid,output='small'):void{for(let i=0;i<count;i++)manager.queueObservation(session,{tool_name:'Read',tool_input:{file_path:`file-${i}.ts`},tool_response:output,
  toolUseId:`${session}-tool-${i}`,prompt_number:i%3+1,cwd:i%2?'D:/work/second':'D:/work/first',agentId:i%4?`agent-${i%2}`:undefined,agentType:i%4?'general-purpose':undefined},1000+i);}
async function run(session=sid):Promise<void>{currentSession=manager.initializeSession(session);currentSession.currentProvider='claude';await new ClaudeProvider(dbManager,manager).startSession(currentSession);}
function rows(sql:string):Array<Record<string,unknown>>{return store.db.prepare(sql).all() as Array<Record<string,unknown>>;}

describe('Claude v2 actual SDK send path',()=>{
  it('35 mixed events send exactly 16/16/3; final summary shares the third request and all sources persist',async()=>{
    add(35);manager.queueSummarize(sid,'finished',3,2000);
    await run();
    expect(sends.map(s=>s.batch.entries.filter(e=>e.message.type==='observation').length)).toEqual([16,16,3]);
    expect(sends.map(s=>s.batch.entries.filter(e=>e.message.type==='summarize').length)).toEqual([0,0,1]);
    expect(rows('SELECT * FROM observations')).toHaveLength(35);
    expect(rows('SELECT * FROM observation_source_events')).toHaveLength(35);
    expect(rows('SELECT * FROM summary_source_events')).toHaveLength(1);
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state!='done'")).toHaveLength(0);
    expect(rows('SELECT SUM(discovery_tokens) AS tokens FROM observations')[0].tokens).toBe(0);
    expect(rows('SELECT event_count FROM observer_batch_runs').map(r=>r.event_count)).toEqual([16,16,3]);
    expect(rows('SELECT reason FROM observer_batch_runs').map(r=>r.reason)).toEqual(['event_limit','event_limit','queue_drained']);
    expect(rows(`SELECT o.id FROM observations o JOIN observation_source_events l ON l.observation_id=o.id
      JOIN observer_deferred_outbox q ON q.id=l.event_id GROUP BY o.id HAVING o.created_at_epoch!=MIN(json_extract(q.source_meta_json,'$.occurred_at_epoch'))`)).toHaveLength(0);
    expect(rows('SELECT usage_json FROM observer_batch_runs').every(r=>JSON.parse(String(r.usage_json)).input_tokens===100)).toBe(true);
    expect(currentSession.lastSummaryStored).toBe(true);
  });
  it('isolates original sessions even in same project, and isolates other projects',async()=>{
    const second=store.createSDKSession('content-two','note','two');store.updateMemorySessionId(second,'memory-two');
    const third=store.createSDKSession('content-three','SkillHub','three');store.updateMemorySessionId(third,'memory-three');
    for(const id of [sid,second,third]){add(2,id);manager.queueSummarize(id,'done',2);await run(id);}
    expect(sends).toHaveLength(3);
    expect(sends.every(s=>s.batch.entries.every(e=>e.source.session_db_id===s.session&&e.source.project===s.batch.project))).toBe(true);
    expect(new Set(rows('SELECT memory_session_id FROM observations').map(r=>r.memory_session_id)).size).toBe(3);
  });
  it('multi-source and one-to-many preserve mixed NULLs, unknown agents, exact event times and per-event cwd',async()=>{
    add(3);manager.queueSummarize(sid,'done',3);
    answer=batch=>obs(batch.entries.slice(0,2).map(e=>e.label),'mixed')+obs([batch.entries[2].label],'single-a')+obs([batch.entries[2].label],'single-b')+summary([batch.entries[3].label]);
    await run();
    const mixed=rows("SELECT * FROM observations WHERE title='mixed'")[0];
    expect([mixed.agent_id,mixed.agent_type,mixed.prompt_number]).toEqual([null,null,null]);
    expect(mixed.created_at_epoch).toBe(1000);
    expect(JSON.parse(String(mixed.files_read)).sort()).toEqual(['D:\\work\\first\\file-0.ts','D:\\work\\second\\file-1.ts']);
    expect(JSON.parse(String(mixed.metadata)).attribution).toBe('unknown');
    expect(rows('SELECT * FROM observation_source_events')).toHaveLength(4);
  });
  it('marks a complete mixture of known platform agents as mixed and omits ambiguous relative model paths',async()=>{
    for(let i=0;i<2;i++)manager.queueObservation(sid,{tool_name:'Bash',tool_input:{command:'done'},tool_response:'done',toolUseId:`known-${i}`,
      prompt_number:i+1,cwd:i?'D:/two':'D:/one',agentId:`actual-${i}`,agentType:'general-purpose'},5000+i);
    manager.queueSummarize(sid,'done',2);
    answer=batch=>obs(batch.entries.slice(0,2).map(e=>e.label),'known-mixture').replace('<files_read></files_read>','<files_read><file>ambiguous.ts</file></files_read>')+summary([batch.entries[2].label]);
    await run();const row=rows('SELECT * FROM observations')[0];expect([row.agent_id,row.agent_type,row.prompt_number]).toEqual([null,null,null]);
    expect(JSON.parse(String(row.metadata)).attribution).toBe('mixed');expect(JSON.parse(String(row.files_read))).toEqual([]);
  });
  it('merges multiple summary rounds and makes repeated Stop boundaries idempotent',async()=>{
    add(1);manager.queueSummarize(sid,'first',1);manager.queueSummarize(sid,'first',1);
    manager.queueObservation(sid,{tool_name:'Bash',tool_input:{command:'next'},tool_response:'next',toolUseId:'next',prompt_number:2});
    manager.queueSummarize(sid,'second',2);await run();
    expect(sends).toHaveLength(1);expect(rows('SELECT prompt_number FROM session_summaries').map(r=>r.prompt_number)).toEqual([1,2]);
  });
  for(const invalid of ['missing','unknown','cross-batch','tail-missing','conflicting-skip'])it(`quarantines ${invalid} attribution and never sends a paid retry`,async()=>{
    add(2);manager.queueSummarize(sid,'done',2);
    answer=batch=>invalid==='missing'?obs([batch.entries[0].label]).replace(/<source_event_ids>[\s\S]*?<\/source_event_ids>/,''):
      invalid==='unknown'?obs(['E999']):invalid==='cross-batch'?obs(['BpreviousE1']):invalid==='tail-missing'?obs([batch.entries[0].label]):
        obs(batch.entries.map(e=>e.label))+`<skipped_event_ids><source>${batch.entries[0].label}</source></skipped_event_ids>`;
    await run();expect(sends).toHaveLength(1);expect(rows('SELECT * FROM observations')).toHaveLength(0);
    expect(rows('SELECT status,response_text FROM observer_batch_runs')[0].status).toBe('attribution_error');
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state='parked'")).toHaveLength(3);
  });
  it('sends fields over 16k directly and splits a >96k event into two recoverable requests without compressor calls',async()=>{
    add(1,sid,'x'.repeat(40000));manager.queueSummarize(sid,'done',1);await run();expect(sends).toHaveLength(1);
    const second=store.createSDKSession('huge','note','huge');store.updateMemorySessionId(second,'huge-memory');
    add(1,second,'y'.repeat(110000));manager.queueSummarize(second,'done',1);await run(second);
    expect(sends.filter(s=>s.session===second)).toHaveLength(2);
    expect(rows(`SELECT * FROM observer_event_fragments WHERE event_id IN (SELECT id FROM observer_deferred_outbox WHERE session_db_id=${second} AND message_type='observation')`)).toHaveLength(2);
    expect(sends.every(s=>s.prompt.length<=96000)).toBe(true);
  });
  it('rolls back records, source links and done state on DB failure while preserving paid output and parking claims',async()=>{
    add(1);manager.queueSummarize(sid,'done',1);
    store.db.exec("CREATE TRIGGER fail_source BEFORE INSERT ON observation_source_events BEGIN SELECT RAISE(ABORT,'test fault'); END");
    await expect(run()).rejects.toThrow('test fault');expect(sends).toHaveLength(1);
    expect(rows('SELECT * FROM observations')).toHaveLength(0);expect(rows('SELECT * FROM observation_source_events')).toHaveLength(0);
    expect(rows('SELECT status,response_text FROM observer_batch_runs')[0].status).toBe('storage_error');
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state='done'")).toHaveLength(0);
  });
  it('commit-before-RAM-confirm is idempotent and a committed batch does not buy another request on recovery',async()=>{
    add(1);manager.queueSummarize(sid,'done',1);await run();const sent=sends[0];
    const before=rows('SELECT * FROM session_summaries').length;
    const replay=commitBatchOutput(store,'memory-one',sent.batch,validateBatchOutput(answer(sent.batch),sent.batch),sanitizeV2ObservationFiles);
    expect(replay.replay).toBe(true);expect(rows('SELECT * FROM session_summaries')).toHaveLength(before);
    const released:number[]=[];const recovered=new DeferredObserverQueue(store.db,0,rs=>released.push(...rs.map(r=>r.id)));recovered.recover();recovered.stop();
    expect(released).toEqual([]);expect(sends).toHaveLength(1);
  });
  it('keeps old row metadata/time when identical legacy content is reused; old source stays explicitly incomplete',async()=>{
    store.storeObservations('memory-one','note',[{type:'discovery',title:'legacy',subtitle:null,facts:['old'],narrative:'done',concepts:[],files_read:[],files_modified:[],
      agent_id:'old-agent',agent_type:'old-type',metadata:JSON.stringify({custom:'keep'})}],null,9,7,100);
    add(1);manager.queueSummarize(sid,'done',1);answer=batch=>obs([batch.entries[0].label],'legacy')+summary([batch.entries[1].label]);await run();
    const row=rows("SELECT * FROM observations WHERE title='legacy'")[0];
    expect(row.created_at_epoch).toBe(100);expect(row.agent_id).toBe(null);expect(row.prompt_number).toBe(null);
    const meta=JSON.parse(String(row.metadata));expect(meta.custom).toBe('keep');expect(meta.source_incomplete).toBe(true);expect(meta.legacy_source.agent_id).toBe('old-agent');
    expect((chromaCalls[0][3] as {facts:string[]}).facts).toEqual(['old']);
    expect(chromaCalls[0][4]).toBe(null);expect(chromaCalls[0][5]).toBe(100);
  });
  for (const outcome of ['eof','error'] as const) it(`parks an uncertain ${outcome} send and never recovers it for another paid attempt`,async()=>{
    add(1);manager.queueSummarize(sid,'done',1);sdkOutcome=outcome;await run();
    expect(sends).toHaveLength(1);
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state='parked'")).toHaveLength(2);
    const released:number[]=[];const recovered=new DeferredObserverQueue(store.db,0,rs=>released.push(...rs.map(r=>r.id)));recovered.recover();recovered.stop();
    expect(released).toHaveLength(0);
  });
  it('resumes only unsent long-event fragments after a committed first piece and keeps the original incomplete until every piece commits',async()=>{
    add(1,sid,'z'.repeat(110000));manager.queueSummarize(sid,'done',1);
    onAfterResult=()=>{currentSession.abortReason='shutdown';currentSession.abortController.abort();};await run();
    expect(sends).toHaveLength(1);
    expect(rows("SELECT state FROM observer_deferred_outbox WHERE message_type='observation'")[0].state).toBe('released');
    expect(rows("SELECT * FROM observer_event_fragments WHERE state='done'")).toHaveLength(1);
    await manager.shutdownAll();manager=new SessionManager(dbManager);currentManager=manager;
    manager.configureDeferredObserver(store.db,{CLAUDE_MEM_OBSERVER_TRIGGER:'summary_or_session_end',CLAUDE_MEM_OBSERVER_MAX_WAIT_MS:'0'});
    onAfterResult=undefined;await run();
    expect(sends).toHaveLength(2);
    expect(sends[1].batch.entries.find(e=>e.message.type==='observation')?.fragmentIndex).toBe(1);
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state!='done'")).toHaveLength(0);
  });
  it('quarantines remaining unsent pieces of a rejected original event',async()=>{
    add(1,sid,'z'.repeat(110000));manager.queueSummarize(sid,'done',1);
    answer=batch=>batch.entries.some(e=>e.message.type==='observation')?obs(['FORGED']):summary([batch.entries[0].label]);await run();
    expect(sends.filter(s=>s.batch.entries.some(e=>e.message.type==='observation'))).toHaveLength(1);
    expect(rows("SELECT state FROM observer_deferred_outbox WHERE message_type='observation'")[0].state).toBe('parked');
  });
  it('records explicit all-skipped coverage without manufacturing observations or standalone summary requests',async()=>{
    add(2);manager.queueSummarize(sid,'done',1);
    answer=batch=>`<skipped_event_ids>${batch.entries.map(e=>`<source>${e.label}</source>`).join('')}</skipped_event_ids>`;
    await run();expect(sends).toHaveLength(1);expect(rows('SELECT * FROM observations')).toHaveLength(0);
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state!='done'")).toHaveLength(0);
    expect(sends[0].prompt).toContain('Noise-only output example: <skipped_event_ids>');
    expect(sends[0].prompt).not.toContain('<skip_summary');
    expect(sends[0].prompt).toContain('List EVERY skipped input label');
  });
  it('keeps media results and image manifests scoped to each cited event and preserves v2 metadata',async()=>{
    const attachmentIds=['11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222'];
    for(let i=0;i<2;i++){
      const key=`media-${i}`;
      store.db.prepare(`INSERT INTO media_events(event_key,session_db_id,platform,event_identity,created_at) VALUES(?,?,?,?,?)`).run(key,sid,'claude',key,1000+i);
      store.db.prepare(`INSERT INTO media_attachments(id,replay_key,provenance,recipe,state,created_at) VALUES(?,?,?,'screenshot-v1','ready',?)`).run(attachmentIds[i],key,'{}',1000+i);
      store.db.prepare('INSERT INTO media_event_refs(event_key,attachment_id,label) VALUES(?,?,?)').run(key,attachmentIds[i],'event1_image1');
      manager.queueObservation(sid,{tool_name:'Read',tool_input:{file_path:`image-${i}.png`},tool_response:'captured',toolUseId:key,prompt_number:1,
        mediaEventKey:key,mediaRefs:[{id:attachmentIds[i],label:'event1_image1',inspection:'uninspected'}]},1000+i);
    }
    manager.queueSummarize(sid,'done',1);
    const iterator=manager.getMessageIterator(sid);const first=(await iterator.next()).value!;
    const messages=[first];for(;;){const next=manager.claimNextV2Message(sid,()=>true);if(!next)break;messages.push(next);}
    const id=newBatchId();const entries=messages.map((message,index)=>makeBatchEntry(message,index+1,id,'note',sid));
    const batch:ObserverBatch={id,project:'note',sessionDbId:sid,entries,prompt:renderBatch(entries),reason:'queue_drained',requestKind:'mixed'};
    beginBatch(store.db,batch);currentSession=manager.initializeSession(sid);currentSession.memorySessionId='memory-one';
    const withAttachment=(text:string,label:string)=>text.replace('</observation>',`<attachments><attachment>${label}</attachment></attachments></observation>`);
    const text=withAttachment(obs([entries[0].label],'image-one'),'event1_image1')+withAttachment(obs([entries[1].label],'image-two'),'event2_image1')
      +withAttachment(obs([entries[1].label],'forged-other-image'),'event1_image1')+summary([entries[2].label]);
    await processAgentResponse(text,currentSession,dbManager,manager,undefined,0,null,'SDK',undefined,undefined,{
      project:'note',promptNumber:1,pendingAgentId:null,pendingAgentType:null,batchV2:batch,
      media:{imagesDelivered:true,events:entries.slice(0,2).map((entry,index)=>({eventKey:entry.message.mediaEventKey!,ordinal:index+1})),
        images:attachmentIds.map((attachmentId,index)=>({attachmentId,requestLabel:`event${index+1}_image1`,eventOrdinal:index+1,eventKey:`media-${index}`,eventLabel:'event1_image1'}))}});
    const observations=rows('SELECT id,title,metadata FROM observations');
    const firstRow=observations.find(r=>r.title==='image-one')!;const secondRow=observations.find(r=>r.title==='image-two')!;
    expect(rows('SELECT * FROM observation_media_links')).toHaveLength(2);
    expect(JSON.parse(String(firstRow.metadata)).cmem_media_v1.attachments[0].id).toBe(attachmentIds[0]);
    expect(JSON.parse(String(secondRow.metadata)).cmem_media_v1.attachments[0].id).toBe(attachmentIds[1]);
    expect(JSON.parse(String(firstRow.metadata)).source_schema).toBe('event-v2');
    expect(JSON.parse(String(observations.find(r=>r.title==='forged-other-image')!.metadata)).cmem_media_v1).toBeUndefined();
    expect(rows("SELECT * FROM media_event_results WHERE event_key='media-0'").map(r=>r.observation_id)).toEqual([firstRow.id]);
    expect(rows("SELECT * FROM observer_deferred_outbox WHERE state!='done'")).toHaveLength(0);
  });
  it('parks a fresh-window payload that cannot fit the effective model budget without buying or truncating a request',async()=>{
    process.env.CLAUDE_MEM_OBSERVER_CONTEXT_WINDOW='8192';
    add(1,sid,'x'.repeat(40000));manager.queueSummarize(sid,'done',1);await run();
    expect(sends).toHaveLength(0);
    expect(rows("SELECT reason,message_json FROM observer_deferred_outbox WHERE message_type='observation'")[0].reason).toBe('input_budget_requires_refragment');
    expect(String(rows("SELECT message_json FROM observer_deferred_outbox WHERE message_type='observation'")[0].message_json)).toContain('x'.repeat(40000));
  });
});
