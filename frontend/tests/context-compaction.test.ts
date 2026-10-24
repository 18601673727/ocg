import assert from "node:assert/strict";
import test from "node:test";
import { ContractError, decodeContextStatus, decodeProfiles, decodeCompactionPolicy, type ControlCompactionPolicy } from "../components/ocg/contracts";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";

const id = "00000001-0000-7001-8000-000000000001";
const model = "00000001-0000-7001-8000-000000000002";
const agent = "00000001-0000-7001-8000-000000000003";
const off: ControlCompactionPolicy = {mode:{kind:"off"},summarization_model:null,predictive:null};
const profiles = {api_version:1,llm_profiles:[],agent_profiles:[],decision_profiles:[],decision_agent_profiles:[],executor_protocols:[],providers:[],has_more:false,next_before:null};
const limits = {max_input_tokens_per_request:null,max_output_tokens_per_request:null,max_input_tokens_per_question:null,max_output_tokens_per_question:null};

test("Decision Profiles survive paged catalog merging with their model versions",async()=>{
  const client=createHttpCanonicalControlClient({baseUrl:"http://localhost:7841",fetch:async url=>Response.json(String(url).includes("before=")?{...profiles,decision_agent_profiles:[{id:agent,name:"predictor",instructions:null,models:[model],default_model:model,workspace_access:"none",editable:true,created_at_us:1,updated_at_us:1}]}:{...profiles,has_more:true,next_before:model,decision_profiles:[{id:model,edit_seq:3,name:"native",model:"provider-version",protocol:"typesafe_systemone",endpoint:null,credential_ref:null,has_credential:false,declared_capabilities:["choice"],limits,editable:true,created_at_us:1,updated_at_us:1}]})});
  const result=await client.readProfiles();
  assert.equal(result.decision_profiles[0].edit_seq,3);
  assert.equal(result.decision_agent_profiles[0].models[0],model);
  client.close();
  assert.throws(()=>decodeProfiles({...profiles,decision_profiles:[{id:model,edit_seq:"3"}]}),ContractError);
});

test("Compaction controls use signed canonical routes and retain the supplied retry key",async()=>{
  const calls:{url:string;key:string|null;body:unknown}[]=[];
  const client=createHttpCanonicalControlClient({baseUrl:"http://localhost:7841",csrfToken:()=>"csrf",fetch:async(url,init)=>{
    assert.equal(new Headers(init?.headers).get("X-OCG-CSRF"),"csrf");
    calls.push({url:String(url),key:new Headers(init?.headers).get("Idempotency-Key"),body:JSON.parse(String(init?.body))});
    if(String(url).endsWith("/compact"))return Response.json({api_version:1,conversation:id,status:"queued",job:model,system_conversation:agent,through:model,subsequent:true,reason:null});
    return Response.json({api_version:1,conversation:id,already:false});
  }});
  const key="context-retry-key-123456";
  await client.compactConversation(id,model,key); await client.compactConversation(id,model,key);
  assert.equal(calls[0].url,`http://localhost:7841/api/v1/conversations/${id}/compact`);
  assert.deepEqual(calls[0].body,{model}); assert.equal(calls[0].key,calls[1].key);
  client.close();
});

test("Context projection validates typed work and does not accept an unrelated conversation",()=>{
  const state={api_version:1,conversation:id,mode:"disabled",summarizer:null,predictive:false,predictive_unavailable:null,revision:0,uncalibrated:true,policy:off,policy_override:null,policy_source:"host",predictor:null,predictor_model:null,prediction_mode:"disabled",predictive_preparation_enabled:false,foreground_model:null,estimated_tokens:null,estimate_method:"estimate",recent_growth_tokens:0,last_context_use:null,work:[],has_more:false,next_before:null};
  assert.equal(decodeContextStatus(state,id).policy_source,"host");
  assert.throws(()=>decodeContextStatus({...state,conversation:model},id),ContractError);
  assert.throws(()=>decodeContextStatus({...state,work:[{kind:"unknown"}]},id),ContractError);
  assert.deepEqual(decodeCompactionPolicy(off),off);
  assert.throws(()=>decodeCompactionPolicy({...off,mode:{kind:"auto",preset:"invented"}}),ContractError);
});
