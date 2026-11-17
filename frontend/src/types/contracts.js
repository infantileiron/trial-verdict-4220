/** @typedef {{id:number,code:string,name:string,starts_at:string,ends_at:string,a_ratio:number,hypothesis:string}} Experiment */
/** @typedef {{id:number,experiment_id:number,user_key:string,group:'A'|'B',kind:'exposure'|'activation',occurred_at:string}} TrialEvent */
/** @typedef {{id:number,experiment_id:number,state:string,result:object,snapshot:object,rationale:string,action:string,published_at:string}} Decision */
/** @typedef {{id:number,experiment_id:number,state:'queued'|'migrating'|'done'|'failed',watermark:number,source_count:number,event_count:number,counts:object,checksum:string,error:string,diff:string,attempts:number,created_at:string,finished_at:string}} ArchiveBatch */
/** @typedef {{experiment_id:number,code:string,name:string,online:object,archived:object,total:object,latest_batch:ArchiveBatch|null,can_archive:boolean,can_retry:boolean,integrity_errors:Array,failed_batch:ArchiveBatch|null}} ArchiveSummary */
export const CONTRACT_VERSION='1.1';
