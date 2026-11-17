"""归档队列操作：与裁决共用 Redis 持久队列，失败保留在线原件并可原地恢复重试。"""
import os,redis
from fastapi import HTTPException
from src.models.entities import ArchiveBatch,Experiment,Event
from src.services.archive import ArchiveError,now,release_events,source_events,write_archive,verify_source,read_archive,event_keys,checksum_of,counts_of,archive_path
from src.services.business import dump

QUEUE='trial:archives'

def client():return redis.Redis.from_url(os.environ['REDIS_URL'])

def enqueue(batch):
 try:client().rpush(QUEUE,batch.id)
 except redis.RedisError as e:raise HTTPException(503,f'归档队列暂不可用，恢复后重试：{e}')

def create(s,exp,user):
 from src.services.archive import prepare_batch,read_archive
 s.query(Experiment).filter_by(id=exp.id).with_for_update().first()
 active=s.query(ArchiveBatch).filter(ArchiveBatch.experiment_id==exp.id,ArchiveBatch.state.in_(['queued','migrating'])).first()
 if active:raise HTTPException(409,f'该实验已有进行中的归档批次 #{active.id}（{active.state}），请等待完成')
 failed=s.query(ArchiveBatch).filter_by(experiment_id=exp.id,state='failed').first()
 if failed:raise HTTPException(409,f'该实验存在失败批次 #{failed.id}，请先恢复重试，避免重复归档')
 for b in s.query(ArchiveBatch).filter_by(experiment_id=exp.id,state='done').all():
  try:read_archive(b)
  except ArchiveError as e:raise HTTPException(409,f'现有归档批次 #{b.id} 已损坏或不完整，拒绝继续归档以免缩小样本：{e}')
 try:b,_=prepare_batch(s,exp,user)
 except ArchiveError as e:raise HTTPException(409,str(e))
 s.commit()
 try:enqueue(b)
 except HTTPException as e:
  b.state='failed';b.error=str(e.detail);b.finished_at=now();s.commit();raise
 return b

def retry(s,batch,user):
 """失败批次原地恢复：失败时未提交释放，在线原件完整，重新冻结当前在线集合再跑一次。"""
 if batch.state!='failed':raise HTTPException(409,'仅失败批次需要恢复重试')
 s.query(ArchiveBatch).filter_by(id=batch.id).with_for_update().first()
 active=s.query(ArchiveBatch).filter(ArchiveBatch.experiment_id==batch.experiment_id,ArchiveBatch.id!=batch.id,ArchiveBatch.state.in_(['queued','migrating'])).first()
 if active:raise HTTPException(409,f'该实验已有进行中的归档批次 #{active.id}，请等待完成后再重试')
 idle=s.query(Event).filter_by(experiment_id=batch.experiment_id).order_by(Event.id).all()
 if not idle:raise HTTPException(409,'在线原件已不存在，无法恢复重试，请联系管理员')
 # 失败批次没有释放任何在线明细，可安全重新冻结到当前在线集合。
 batch.ids=[e.id for e in idle];batch.watermark=max(batch.ids);batch.source_count=len(idle);batch.event_count=len(idle)
 batch.counts=counts_of([dump(e) for e in idle]);batch.checksum=checksum_of([dump(e) for e in idle])
 batch.path=str(archive_path(batch));batch.state='queued';batch.error='';batch.started_at='';batch.finished_at=''
 batch.attempts=(batch.attempts or 0)+1;s.commit()
 try:enqueue(batch)
 except HTTPException as e:
  batch.state='failed';batch.error=str(e.detail);s.commit();raise
 return batch

def _audit(s,action):
 from src.models.entities import Audit
 s.add(Audit(actor='system:archive-worker',action=action,created=now()));s.commit()

def process(s,batch):
 """queued → migrating → 写文件 → 核验 → 原子释放 → done。在线原件保留到核验通过。

 写文件与核验不持有行锁，采集可并发进行；释放阶段再短暂锁实验行，按冻结事件 ID
 复核并删除，最后与 done 状态在同一事务提交。
 """
 if batch.state!='queued':return
 batch.state='migrating';batch.started_at=batch.started_at or now();batch.error='';s.commit()
 try:
  events=[dump(e) for e in source_events(s,batch.experiment_id,batch.ids)]
  verify_source(batch,events)
  write_archive(batch,events)
  reread=read_archive(batch,use_cache=False)
  if event_keys(reread)!=event_keys(events):raise ArchiveError(f'归档回读与在线原件不一致：批次 #{batch.id}')
  # 短暂加锁后重新读取冻结 ID，确认待释放集合与已核验内容一致。
  s.commit()
  s.query(Experiment).filter_by(id=batch.experiment_id).with_for_update().first()
  current=[dump(e) for e in source_events(s,batch.experiment_id,batch.ids)]
  if checksum_of(current)!=batch.checksum or len(current)!=batch.source_count:
   raise ArchiveError(f'释放前在线原件与已核验归档不一致：批次 #{batch.id}（核验 {len(events)} 条，当前 {len(current)} 条）；差异说明：归档后在线副本被改动，在线原件保留')
  released=release_events(s,batch.experiment_id,batch.ids)
  if released!=len(events):raise ArchiveError(f'释放在线明细数量异常：批次 #{batch.id} 应释放 {len(events)} 条，实际 {released} 条；已回滚并保留原件')
  batch.event_count=len(events);batch.finished_at=now();batch.state='done';s.commit()
  _audit(s,f'完成事件归档 批次{batch.id} 实验{batch.experiment_id} 释放{released}条')
 except ArchiveError as e:
  s.rollback();row=s.get(ArchiveBatch,batch.id);row.state='failed';row.error=str(e);row.finished_at=now();s.commit()
  _audit(s,f'归档失败 批次{batch.id}：{e}')
 except Exception as e:
  s.rollback();row=s.get(ArchiveBatch,batch.id);row.state='failed';row.error=f'归档迁移异常：{e}';row.finished_at=now();s.commit()
  _audit(s,f'归档异常 批次{batch.id}：{e}')
