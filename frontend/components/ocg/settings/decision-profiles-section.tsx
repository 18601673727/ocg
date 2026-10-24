"use client";

import { useState, type FormEvent } from "react";
import type { ControlDecisionLimits, ControlProfilesSnapshot } from "../contracts";
import type { HttpCanonicalControlClient, ControlProfileWriteRequest } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { Section, describeError } from "./settings-parts";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { DismissibleNotice } from "../primitives/dismissible-notice";

const emptyLimits: ControlDecisionLimits = {max_input_tokens_per_request:null,max_output_tokens_per_request:null,max_input_tokens_per_question:null,max_output_tokens_per_question:null};
export function DecisionProfilesSection({client,profiles,onSaved}: {client:HttpCanonicalControlClient;profiles:ControlProfilesSnapshot;onSaved(profiles:ControlProfilesSnapshot):void}) {
  const {t} = useI18n();
  const [kind,setKind]=useState<"decision_model"|"decision_agent">("decision_model");
  const [editing,setEditing]=useState<string>();
  const [name,setName]=useState("");
  const [model,setModel]=useState("");
  const [endpoint,setEndpoint]=useState("");
  const [credential,setCredential]=useState("");
  const [credentialRef,setCredentialRef]=useState<string|null>(null);
  const [capabilities,setCapabilities]=useState<string[]>(["choice"]);
  const [limits,setLimits]=useState<ControlDecisionLimits>(emptyLimits);
  const [instructions,setInstructions]=useState("");
  const [models,setModels]=useState<string[]>([]);
  const [defaultModel,setDefaultModel]=useState<string|null>(null);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState<string>();
  function reset(kind: "decision_model"|"decision_agent") {setKind(kind);setEditing(undefined);setName("");setModel("");setEndpoint("");setCredential("");setCredentialRef(null);setCapabilities(["choice"]);setLimits(emptyLimits);setInstructions("");setModels([]);setDefaultModel(null);setError(undefined);}
  async function save(event:FormEvent) {
    event.preventDefault();setBusy(true);setError(undefined);
    const payload:ControlProfileWriteRequest=kind==="decision_model"?{kind,name:name.trim(),model:model.trim(),endpoint:endpoint.trim(),protocol:"typesafe_systemone",declared_capabilities:capabilities,limits,credential_ref:credentialRef,...(credential?{credential}:{})}:{kind,name:name.trim(),instructions:instructions.trim()||null,models,default_model:defaultModel,workspace_access:"none"};
    try {const result=editing?await client.updateProfile(editing,payload):await client.createProfile(payload);onSaved(result);reset(kind);}
    catch(cause){setError(describeError(cause,t));}
    finally{setBusy(false);setCredential("");}
  }
  return <Section id="settings-decision-profiles" title={t("decision.title")} description={t("decision.description")}>
    <div className="divide-y divide-border py-2">
      {profiles.decision_profiles.map(profile=><div key={profile.id} className="flex items-center justify-between gap-2 py-2 text-sm"><span>{profile.name} · {profile.model}</span><Button size="sm" variant="ghost" disabled={busy||!profile.editable} onClick={()=>{reset("decision_model");setEditing(profile.id);setName(profile.name);setModel(profile.model);setEndpoint(profile.endpoint??"");setCredentialRef(profile.credential_ref);setCapabilities(profile.declared_capabilities);setLimits(profile.limits);}}>{t("decision.edit")}</Button></div>)}
      {profiles.decision_agent_profiles.map(profile=><div key={profile.id} className="flex items-center justify-between gap-2 py-2 text-sm"><span>{profile.name} · {t("decision.agent")}</span><Button size="sm" variant="ghost" disabled={busy||!profile.editable} onClick={()=>{reset("decision_agent");setEditing(profile.id);setName(profile.name);setInstructions(profile.instructions??"");setModels(profile.models);setDefaultModel(profile.default_model);}}>{t("decision.edit")}</Button></div>)}
    </div>
    <form onSubmit={event=>void save(event)} className="space-y-3 py-3">
      <fieldset disabled={busy} className="space-y-3">
        <div className="flex gap-2"><Button type="button" variant={kind==="decision_model"?"default":"outline"} onClick={()=>reset("decision_model")}>{t("decision.model")}</Button><Button type="button" variant={kind==="decision_agent"?"default":"outline"} onClick={()=>reset("decision_agent")}>{t("decision.agent")}</Button>{editing&&<Button type="button" variant="ghost" onClick={()=>reset(kind)}>{t("decision.new")}</Button>}</div>
        <label className="grid gap-1 text-xs">{t("decision.name")}<Input required value={name} onChange={event=>setName(event.target.value)} /></label>
        {kind==="decision_model"?<>
          <label className="grid gap-1 text-xs">{t("decision.modelId")}<Input required value={model} onChange={event=>setModel(event.target.value)} /></label>
          <label className="grid gap-1 text-xs">{t("decision.endpoint")}<Input required type="url" value={endpoint} onChange={event=>setEndpoint(event.target.value)} /></label>
          <label className="grid gap-1 text-xs">{t("decision.credential")}<Input type="password" autoComplete="new-password" value={credential} onChange={event=>setCredential(event.target.value)} /></label>
          <fieldset className="space-y-2"><legend className="text-xs">{t("decision.capabilities")}</legend>{(["choice","binary_probability","ordinal_score"] as const).map(capability=><label key={capability} className="mr-3 inline-flex items-center gap-1 text-xs"><input type="checkbox" checked={capabilities.includes(capability)} onChange={event=>setCapabilities(event.target.checked?[...capabilities,capability]:capabilities.filter(value=>value!==capability))} />{t(`decision.${capability}`)}</label>)}</fieldset>
          <details><summary className="cursor-pointer text-xs">{t("decision.limits")}</summary><div className="mt-2 grid grid-cols-2 gap-2">{(["max_input_tokens_per_request","max_output_tokens_per_request","max_input_tokens_per_question","max_output_tokens_per_question"] as const).map(field=><label key={field} className="grid gap-1 text-xs">{t(`decision.${field}`)}<Input type="number" min={1} max={1_000_000} step={1} value={limits[field]??""} onChange={event=>setLimits({...limits,[field]:event.target.value?event.currentTarget.valueAsNumber:null})} /></label>)}</div></details>
        </>:<>
          <label className="grid gap-1 text-xs">{t("decision.instructions")}<Textarea value={instructions} onChange={event=>setInstructions(event.target.value)} /></label>
          <fieldset className="space-y-2"><legend className="text-xs">{t("decision.model")}</legend>{profiles.decision_profiles.map(model=><label key={model.id} className="flex items-center gap-2 text-xs"><input type="checkbox" checked={models.includes(model.id)} onChange={event=>{setModels(event.target.checked?[...models,model.id]:models.filter(value=>value!==model.id));if(defaultModel===model.id&&!event.target.checked)setDefaultModel(null);}} />{model.name}</label>)}</fieldset>
          <label className="grid gap-1 text-xs">{t("decision.model")}<select className="min-h-10 rounded-md border bg-background px-2" value={defaultModel??""} onChange={event=>setDefaultModel(event.target.value||null)}><option value="">{t("decision.firstModel")}</option>{profiles.decision_profiles.filter(model=>models.includes(model.id)).map(model=><option key={model.id} value={model.id}>{model.name}</option>)}</select></label>
          <p className="text-xs text-muted-foreground">{t("decision.inputOnly")}</p>
        </>}
        {error&&<DismissibleNotice message={error} />}
        <Button type="submit" disabled={!name.trim()||(kind==="decision_model"?!capabilities.length:!models.length)}>{busy?t("context.saving"):t("common.save")}</Button>
      </fieldset>
    </form>
  </Section>;
}
