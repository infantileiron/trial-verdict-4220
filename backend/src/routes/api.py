import os,json
from datetime import timezone
import redis
from fastapi import APIRouter,Depends,HTTPException,Response
from sqlalchemy.exc import IntegrityError
from src.config.database import db
from src.models.entities import User,Experiment,Event,Decision,Audit,ArchiveBatch
from src.schemas.business import Login,ExperimentInput,EventInput,RunInput,RationaleInput,PublishInput,ArchiveInput
from src.middlewares.auth import current,editor,admin
from src.utils.security import verify,token
from src.services.business import audit,dump,utc
from src.services.archive import ArchiveError,archive_summary,batch_payload,merged_events
from src.services import archive_jobs
from src.controllers.business import experiment,decision,validate_event,snapshot,publish
router=APIRouter()
@router.post('/auth/login')
def login(data:Login,s=Depends(db)):
 u=s.query(User).filter_by(email=data.email).first()
 if not u or not verify(data.password,u.password):raise HTTPException(401,'账号或密码错误')
 return {'token':token(u),'user':dump(u)}
@router.get('/me')
def me(u=Depends(current)):return dump(u)
@router.get('/experiments')
def experiments(u=Depends(current),s=Depends(db)):
 out=[]
 for e in s.query(Experiment).order_by(Experiment.id.desc()).all():
  try:summary=archive_summary(s,e);merged=len(merged_events(s,e.id))
  except ArchiveError as x:summary={'archive_error':str(x)};merged=None
  out.append({**dump(e),'exposures':s.query(Event).filter_by(experiment_id=e.id,kind='exposure').count(),'total_exposures':(summary.get('total') or {}).get('exposures'),'archived_exposures':(summary.get('archived') or {}).get('exposures'),'archive_error':summary.get('archive_error',''),'merged_events':merged})
 return out
@router.post('/experiments')
def create_experiment(data:ExperimentInput,u=Depends(editor),s=Depends(db)):
 d=data.model_dump();d['starts_at']=utc(data.starts_at);d['ends_at']=utc(data.ends_at);r=Experiment(**d);s.add(r)
 try:s.commit()
 except IntegrityError:s.rollback();raise HTTPException(409,'实验编号已存在')
 audit(s,u,'建立实验 '+r.code);return dump(r)
@router.put('/experiments/{i}')
def edit_experiment(i:int,data:ExperimentInput,u=Depends(editor),s=Depends(db)):
 r=experiment(s,i)
 if s.query(Event).filter_by(experiment_id=i).first() or s.query(ArchiveBatch).filter_by(experiment_id=i).first():raise HTTPException(409,'已有事件或归档，禁止事后修改实验定义；请建立新实验')
 d=data.model_dump();d['starts_at']=utc(data.starts_at);d['ends_at']=utc(data.ends_at)
 for k,v in d.items():setattr(r,k,v)
 try:s.commit()
 except IntegrityError:s.rollback();raise HTTPException(409,'实验编号已存在')
 audit(s,u,'修订实验 '+str(i));return dump(r)
@router.get('/events')
def events(experiment_id:int|None=None,include_archived:bool=True,u=Depends(current),s=Depends(db)):
 """事件列表。默认合并在线与归档；每条标注来源。归档不可读时拒绝返回，避免静默缩小样本。"""
 def listing(exp):
  online=[dump(e) for e in s.query(Event).filter_by(experiment_id=exp.id).all()]
  if not include_archived:return [{**e,'archived':False} for e in online]
  try:rows=merged_events(s,exp.id)
  except ArchiveError as e:raise HTTPException(409,f'归档不可读，拒绝读取事件：{e}')
  online_keys={(e['user_key'],e['kind']) for e in online}
  return [{**e,'archived':(e['user_key'],e['kind']) not in online_keys} for e in rows]
 targeted=[experiment(s,experiment_id)] if experiment_id else s.query(Experiment).order_by(Experiment.id.desc()).all()
 rows=[e for exp in targeted for e in listing(exp)]
 return sorted(rows,key=lambda e:e['occurred_at'],reverse=True)
@router.post('/events')
def create_event(data:EventInput,u=Depends(editor),s=Depends(db)):
 # 锁定实验行：与归档释放事务串行，避免“归档核验后、释放时”遗漏或重复记入同一用户事件。
 experiment(s,data.experiment_id)
 s.query(Experiment).filter_by(id=data.experiment_id).with_for_update().first()
 stamp=validate_event(s,data);d=data.model_dump();d['occurred_at']=stamp;r=Event(**d);s.add(r)
 try:s.commit()
 except IntegrityError:s.rollback();raise HTTPException(409,'该用户事件已存在')
 audit(s,u,'采集事件 '+str(r.id));return dump(r)
@router.get('/decisions')
def decisions(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(Decision).order_by(Decision.id.desc())]
@router.get('/archives')
def archives(u=Depends(current),s=Depends(db)):
 return {'summaries':[archive_summary(s,e) for e in s.query(Experiment).order_by(Experiment.id.desc())],'batches':[batch_payload(b) for b in s.query(ArchiveBatch).order_by(ArchiveBatch.id.desc()).limit(50).all()]}
@router.post('/archives')
def create_archive(data:ArchiveInput,u=Depends(editor),s=Depends(db)):
 exp=experiment(s,data.experiment_id);b=archive_jobs.create(s,exp,u);audit(s,u,'发起事件归档 实验'+exp.code+' 批次'+str(b.id));return batch_payload(b)
@router.get('/archives/summary')
def archive_summaries(u=Depends(current),s=Depends(db)):
 return {'summaries':[archive_summary(s,e) for e in s.query(Experiment).order_by(Experiment.id.desc())]}
@router.get('/archives/{i}')
def archive_detail(i:int,u=Depends(current),s=Depends(db)):
 b=s.get(ArchiveBatch,i)
 if not b:raise HTTPException(404,'归档批次不存在')
 exp=experiment(s,b.experiment_id)
 return {**batch_payload(b),'summary':archive_summary(s,exp)}
@router.post('/archives/{i}/retry')
def archive_retry(i:int,u=Depends(editor),s=Depends(db)):
 b=s.get(ArchiveBatch,i)
 if not b:raise HTTPException(404,'归档批次不存在')
 b=archive_jobs.retry(s,b,u);audit(s,u,'恢复归档批次 '+str(i));return batch_payload(b)
@router.get('/archives/{i}/verify')
def archive_verify(i:int,u=Depends(current),s=Depends(db)):
 from src.services.archive import read_archive
 b=s.get(ArchiveBatch,i)
 if not b:raise HTTPException(404,'归档批次不存在')
 if b.state!='done':raise HTTPException(409,'归档尚未完成，无法核验')
 try:events=read_archive(b,use_cache=False)
 except ArchiveError as e:raise HTTPException(409,f'归档核验失败：{e}')
 return {'batch_id':b.id,'ok':True,'event_count':len(events),'checksum':b.checksum,'counts':b.counts,'diff':''}
@router.post('/decisions')
def run(data:RunInput,u=Depends(editor),s=Depends(db)):
 exp=experiment(s,data.experiment_id)
 if s.query(Decision).filter(Decision.state.in_(['queued','running'])).count()>=10:raise HTTPException(429,'队列已满，请等待任务完成')
 r=Decision(experiment_id=exp.id,snapshot=snapshot(s,exp));s.add(r);s.commit()
 try:redis.Redis.from_url(os.environ['REDIS_URL']).rpush('trial:decisions',r.id)
 except redis.RedisError:r.state='failed';r.error='队列暂不可用，恢复后重新提交';s.commit();raise HTTPException(503,r.error)
 audit(s,u,'提交实验裁决 '+str(r.id));return dump(r)
@router.get('/decisions/{i}')
def detail(i:int,u=Depends(current),s=Depends(db)):return dump(decision(s,i))
@router.put('/decisions/{i}/rationale')
def rationale(i:int,data:RationaleInput,u=Depends(editor),s=Depends(db)):
 r=decision(s,i)
 if r.published_at:raise HTTPException(409,'已发布裁决不可覆盖，请重新创建裁决')
 if r.state!='done':raise HTTPException(409,'计算完成后再填写裁决理由')
 r.rationale=data.rationale;s.commit();audit(s,u,'保存裁决理由 '+str(i));return dump(r)
@router.post('/decisions/{i}/publish')
def release(i:int,data:PublishInput,u=Depends(admin),s=Depends(db)):
 r=publish(s,decision(s,i),data.action,u);audit(s,u,'发布裁决 '+str(i)+' '+data.action);return dump(r)
@router.get('/decisions/{i}/export')
def export(i:int,u=Depends(current),s=Depends(db)):
 r=decision(s,i)
 if r.state!='done':raise HTTPException(409,'尚无成功结果')
 return Response(json.dumps(dump(r),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="trial-verdict-{i}.json"'})
@router.get('/audits')
def audits(u=Depends(admin),s=Depends(db)):return [dump(r) for r in s.query(Audit).order_by(Audit.id.desc()).limit(100)]
