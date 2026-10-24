"use client";

import { useEffect, useRef, useState } from "react";
import { useOcgRuntime } from "../runtime/runtime-context";
import type { ControlCompactionPolicy, ControlProfilesSnapshot, decodeContextStatus } from "../contracts";
import { CompactionPolicyFields } from "../settings/compaction-policy-fields";
import { describeError } from "../settings/settings-parts";
import { useI18n } from "../i18n";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { DismissibleNotice } from "../primitives/dismissible-notice";

type Status = ReturnType<typeof decodeContextStatus>;
export function ConversationContext({conversation}: {conversation:string}) {
  const {control}=useOcgRuntime();
  const {t}=useI18n();
  const [open,setOpen]=useState(false);
  const [profiles,setProfiles]=useState<ControlProfilesSnapshot>();
  const [status,setStatus]=useState<Status>();
  const [draft,setDraft]=useState<ControlCompactionPolicy|null>(null);
  const [model,setModel]=useState("");
  const [error,setError]=useState<string>();
  const [notice,setNotice]=useState<string>();
  const [busy,setBusy]=useState(false);
  const [retrying,setRetrying]=useState(false);
  const [refresh,setRefresh]=useState(0);
  const [older,setOlder]=useState<Status["work"]>([]);
  const [before,setBefore]=useState<string|null>(null);
  const request=useRef<{model:string;key:string}|null>(null);
  const initialized=useRef(false);
  useEffect(()=>{
    if(!open||!control)return;
    const controller=new AbortController();
    let timer:ReturnType<typeof setTimeout>|undefined;
    async function tick(){
      try {
        const state=await control!.readContextStatus(conversation,{signal:controller.signal});
        if(controller.signal.aborted)return;
        setStatus(state);
        if(!initialized.current){setDraft(state.policy_override);setModel(state.summarizer??"");setBefore(state.next_before);initialized.current=true;}
        const active=state.work.some(work=>["queued","preparing","predicting"].includes(work.state));
        timer=setTimeout(()=>void tick(),active?3000:10000);
      }catch(cause){if(!controller.signal.aborted)setError(describeError(cause,t));}
    }
    void control.readProfiles(controller.signal).then(value=>{if(!controller.signal.aborted)setProfiles(value);},cause=>{if(!controller.signal.aborted)setError(describeError(cause,t));});
    void tick();
    return()=>{controller.abort();clearTimeout(timer);};
  },[open,control,conversation,refresh,t]);
  async function act(action:()=>Promise<void>){setBusy(true);setError(undefined);setNotice(undefined);try{await action();setRefresh(value=>value+1);}catch(cause){setError(describeError(cause,t));}finally{setBusy(false);}}
  const work=[...(status?.work??[]),...older.filter(work=>!status?.work.some(current=>current.job===work.job))];
  return <>
    <Button type="button" variant="outline" size="sm" disabled={!control} onClick={()=>setOpen(true)}>{t("context.title")}</Button>
    <Dialog open={open} onOpenChange={setOpen}><DialogContent className="max-h-[90dvh] overflow-y-auto sm:max-w-xl">
      <DialogHeader><DialogTitle>{t("context.title")}</DialogTitle><DialogDescription>{t("context.manualHint")}</DialogDescription></DialogHeader>
      {error&&<DismissibleNotice message={error} />}{notice&&<p role="status" className="text-sm">{notice}</p>}
      {status&&<div className="space-y-2 text-xs text-muted-foreground">
        <p>{t("context.usage",{tokens:status.estimated_tokens??t("context.unknown"),capacity:status.foreground_model?.capacity??t("context.unknown"),mode:status.mode,source:status.policy_source})}</p>
        <p>{t("context.preparedMode",{mode:status.prediction_mode})}</p>
        {status.predictive_unavailable&&status.predictive&&<p>{status.predictive_unavailable}</p>}
        <Button variant="ghost" size="sm" disabled={busy} onClick={()=>{initialized.current=false;setOlder([]);setRefresh(value=>value+1);}}>{t("context.refresh")}</Button>
      </div>}
      {profiles&&status&&<>
        <form className="space-y-3" onSubmit={event=>{event.preventDefault();void act(async()=>{await control!.editConversation(conversation,{compaction_policy:draft});setNotice(t("context.saved"));});}}>
          <CompactionPolicyFields key={`${conversation}:${status.revision}`} value={draft} onChange={setDraft} profiles={profiles} disabled={busy} />
          <Button type="submit" disabled={busy}>{t("common.save")}</Button>
        </form>
        <div className="space-y-2 border-t border-border pt-3">
          <label className="grid gap-1 text-xs">{t("context.summarizer")}<select disabled={busy} className="min-h-10 rounded-md border bg-background px-2 text-sm" value={model} onChange={event=>{setModel(event.target.value);request.current=null;setRetrying(false);}}><option value="">{t("context.summarizer")}</option>{profiles.llm_profiles.map(model=><option key={model.id} value={model.id}>{model.name}</option>)}</select></label>
          <Button disabled={busy||!model} variant="outline" onClick={()=>void act(async()=>{
            if(!request.current||request.current.model!==model)request.current={model,key:crypto.randomUUID()};
            setRetrying(true);
            const result=await control!.compactConversation(conversation,model,request.current.key);
            request.current=null;
            setRetrying(false);
            setNotice(result.reason??t("context.queued"));
          })}>{busy?t("context.saving"):retrying?t("context.retry"):t("context.manual")}</Button>
        </div>
      </>}
      {work.length>0&&<ol className="space-y-2 border-t border-border pt-3">{work.map(work=><li key={work.job} className="space-y-1 rounded-md bg-muted p-2 text-xs [overflow-wrap:anywhere]">
        <p>{work.kind=== "summarization"?`${t("context.summarizer")} · ${work.stage}`:t("context.predictor")} · {work.state}</p>
        <p className="font-mono text-muted-foreground">{work.job}</p>
        {work.kind==="summarization"&&<><p>{t("context.sourceRange",{count:work.source.turn_count})} · {work.source.first.turn} → {work.source.through.turn}</p>{work.prediction&&<p>{t("context.reservation",{requests:work.reserved_requests,tokens:work.reserved_tokens})}</p>}</>}
        {work.kind==="summarization"&&["queued","preparing","prepared","part_ready"].includes(work.state)&&<Button size="sm" variant="ghost" disabled={busy} onClick={()=>void act(async()=>{await control!.stopConversation(work.conversation);})}>{t("context.stop")}</Button>}
      </li>)}</ol>}
      {before&&<Button variant="outline" disabled={busy} onClick={()=>void act(async()=>{const page=await control!.readContextStatus(conversation,{before});if(page.next_before&&page.next_before>=before)throw new Error("Context cursor did not advance");setOlder(items=>[...items,...page.work]);setBefore(page.next_before);})}>{t("context.more")}</Button>}
    </DialogContent></Dialog>
  </>;
}
