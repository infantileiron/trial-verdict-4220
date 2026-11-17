"""原始事件归档：把实验的在线事件迁移到不可变归档文件，核验后释放在线明细。

归档文件是权威副本，数据库只保留批次元数据与在线未归档事件。批次迁移期间在线原始
记录仍全部保留，因此采集、校验与裁决不需要读取半成品文件；只有 done 批次才会进入
事件查询与计算样本。
"""
import hashlib,json,os
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import func
from src.models.entities import Event,ArchiveBatch
from src.services.business import dump

class ArchiveError(Exception):
    """归档缺失、损坏或与批次元数据不一致。调用方必须拒绝读取，不能缩小样本。"""

def now():return datetime.now(timezone.utc).isoformat()

def archive_dir():return Path(os.environ.get('ARCHIVE_DIR','/artifacts/archive'))

def canonical(value):return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'))

def checksum_of(events):return hashlib.sha256(canonical(events).encode('utf-8')).hexdigest()

def counts_of(events):
 counts={g:{k:0 for k in ('exposure','activation')} for g in ('A','B')}
 for e in events:
  if e.get('group') in counts and e.get('kind') in ('exposure','activation'):counts[e['group']][e['kind']]+=1
 return counts

def blank_counts():return counts_of([])

def event_keys(events):return [(e['experiment_id'],e['user_key'],e['kind']) for e in events]

def archive_path(batch):return archive_dir()/f'experiment-{batch.experiment_id}-batch-{batch.id}.json'

def source_events(s,experiment_id,ids):
 if not ids:return []
 return s.query(Event).filter(Event.experiment_id==experiment_id,Event.id.in_(ids)).order_by(Event.id).all()

def release_events(s,experiment_id,ids):
 if not ids:return 0
 return s.query(Event).filter(Event.experiment_id==experiment_id,Event.id.in_(ids)).delete(synchronize_session=False)

def prepare_batch(s,exp,user):
 """在实验行锁内固定水位线与确切事件 ID，记录本次归档应有的完整事件集合摘要。"""
 watermark=s.query(func.max(Event.id)).filter(Event.experiment_id==exp.id).scalar()
 if watermark is None:raise ArchiveError('该实验没有可归档的在线事件')
 rows=s.query(Event).filter(Event.experiment_id==exp.id,Event.id<=watermark).order_by(Event.id).all()
 events=[dump(e) for e in rows]
 if not events:raise ArchiveError('该实验没有可归档的在线事件')
 b=ArchiveBatch(experiment_id=exp.id,state='queued',watermark=watermark,ids=[e['id'] for e in events],source_count=len(events),event_count=len(events),counts=counts_of(events),checksum=checksum_of(events),created_by=user.email,created_at=now())
 s.add(b);s.flush();b.path=str(archive_path(b))
 return b,b.path

def _payload(batch,events):
 return {'version':1,'batch_id':batch.id,'experiment_id':batch.experiment_id,'watermark':batch.watermark,'created_at':batch.created_at,'event_count':batch.event_count,'checksum':batch.checksum,'counts':batch.counts,'events':events}

def _write_text(path,text):
 path.parent.mkdir(parents=True,exist_ok=True)
 temp=path.with_name(path.name+'.tmp-'+str(os.getpid()))
 temp.write_text(text,encoding='utf-8')
 os.replace(temp,path)

def write_archive(batch,events):
 """原子写入：先写临时文件再替换，崩溃不会留下可被误判为完整的半文件。"""
 path=Path(batch.path or archive_path(batch))
 _write_text(path,json.dumps(_payload(batch,events),ensure_ascii=False,indent=2))
 return path

_cache={}

def _load_file(batch,use_cache=True):
 path=Path(batch.path or archive_path(batch))
 try:stat=path.stat()
 except FileNotFoundError:raise ArchiveError(f'归档文件缺失：{path.name}，在线原件保留/拒绝计算')
 key=(str(path),stat.st_mtime_ns,stat.st_size)
 cached=_cache.get(key)
 if use_cache and cached is not None and cached[0]==batch.checksum:return cached[1]
 try:payload=json.loads(path.read_text(encoding='utf-8'))
 except Exception as e:raise ArchiveError(f'归档文件无法解析：{path.name}（{e}）')
 events=payload.get('events')
 if not isinstance(events,list):raise ArchiveError(f'归档文件结构无效：{path.name} 缺少 events 列表')
 if use_cache:
  if len(_cache)>256:_cache.clear()
  _cache[key]=(batch.checksum,events)
 return events

def read_archive(batch,use_cache=True):
 """读取并核验批次归档。任何缺失、截断、篡改或数量差异都抛 ArchiveError 并说明差异。"""
 events=_load_file(batch,use_cache)
 actual_checksum=checksum_of(events)
 if actual_checksum!=batch.checksum:
  raise ArchiveError(f'归档校验和不一致：批次 #{batch.id} 期望 {batch.checksum[:12]}…，实际 {actual_checksum[:12]}…；差异说明：内容与归档时冻结的原始事件不一致')
 if len(events)!=batch.event_count:
  raise ArchiveError(f'归档事件数不一致：批次 #{batch.id} 期望 {batch.event_count} 条，实际 {len(events)} 条；差异 {abs(len(events)-batch.event_count)} 条')
 actual_counts=counts_of(events)
 if actual_counts!=(batch.counts or blank_counts()):
  raise ArchiveError(f'归档分组计数不一致：批次 #{batch.id} 期望 {canonical(batch.counts or blank_counts())}，实际 {canonical(actual_counts)}')
 keys=[(e.get('user_key'),e.get('kind')) for e in events]
 if len(set(keys))!=len(keys):raise ArchiveError(f'归档存在重复事件：批次 #{batch.id} 出现重复的 用户+事件类型')
 if any(e.get('experiment_id')!=batch.experiment_id for e in events):raise ArchiveError(f'归档包含其他实验的事件：批次 #{batch.id}')
 return events

def done_batches(s,experiment_id):
 return s.query(ArchiveBatch).filter_by(experiment_id=experiment_id,state='done').order_by(ArchiveBatch.id).all()

def archived_events(s,experiment_id,user_key=None):
 """返回 experiment 的全部已核验归档事件；跨批次重复视为归档损坏。"""
 result=[];seen=set()
 for b in done_batches(s,experiment_id):
  for e in read_archive(b):
   if user_key is not None and e['user_key']!=user_key:continue
   key=(e['user_key'],e['kind'])
   if key in seen:raise ArchiveError(f'跨批次重复归档：用户 {e["user_key"]} 的 {e["kind"]} 同时存在于批次 #{b.id} 与更早批次，已拒绝计算以避免双计')
   seen.add(key);result.append(e)
 return result

def merged_events(s,experiment_id):
 """在线未归档 + 已核验归档 = 完整样本。不静默去重、不缩小样本。"""
 online=[dump(e) for e in s.query(Event).filter_by(experiment_id=experiment_id).order_by(Event.id).all()]
 archived=archived_events(s,experiment_id)
 events=online+archived
 seen={};duplicates=[]
 for e in events:
  key=(e['user_key'],e['kind'])
  if key in seen:duplicates.append(key)
  seen[key]=True
 if duplicates:
  raise ArchiveError(f'在线与归档合并后出现重复事件（用户 {duplicates[0][0]} / {duplicates[0][1]}），已拒绝计算以避免双计')
 groups={}
 for e in events:
  prior=groups.setdefault(e['user_key'],e['group'])
  if prior!=e['group']:raise ArchiveError(f'用户 {e["user_key"]} 同时出现在 A/B 两组（{prior}/{e["group"]}），已拒绝计算，请排查分流污染')
 return events

def prior_events(s,experiment_id,user_key):
 online=[dump(e) for e in s.query(Event).filter_by(experiment_id=experiment_id,user_key=user_key).all()]
 archived=archived_events(s,experiment_id,user_key)
 return online+archived

def verify_source(batch,events):
 if len(events)!=batch.source_count:
  raise ArchiveError(f'迁移期间在线事件数变化：批次 #{batch.id} 冻结 {batch.source_count} 条，当前 {len(events)} 条')
 if checksum_of(events)!=batch.checksum:
  raise ArchiveError(f'迁移期间在线事件内容变化：批次 #{batch.id} 摘要与冻结值不符，已保留在线原件')

def events_counts(events,group,kind):return sum(1 for e in events if e.get('group')==group and e.get('kind')==kind)

def batch_payload(batch):
 d=dump(batch)
 d['diff']=''
 if batch.state=='failed':
  d['diff']=batch.error or '归档失败；在线原件保留'
  if batch.path and not Path(batch.path).exists():d['diff']+='；归档文件未生成或已丢失'
 return d

def archive_summary(s,exp):
 online_events=s.query(Event).filter_by(experiment_id=exp.id).count()
 online_exposures=s.query(Event).filter_by(experiment_id=exp.id,kind='exposure').count()
 archived=blank_counts();archived_events_count=0;integrity=[]
 for b in done_batches(s,exp.id):
  try:events=read_archive(b)
  except ArchiveError as e:integrity.append({'batch_id':b.id,'diff':str(e)});continue
  archived_events_count+=len(events)
  for g in ('A','B'):
   for k in ('exposure','activation'):archived[g][k]+=events_counts(events,g,k)
 latest=s.query(ArchiveBatch).filter_by(experiment_id=exp.id).order_by(ArchiveBatch.id.desc()).first()
 active=s.query(ArchiveBatch).filter_by(experiment_id=exp.id).filter(ArchiveBatch.state.in_(['queued','migrating'])).first()
 failed=s.query(ArchiveBatch).filter_by(experiment_id=exp.id,state='failed').first()
 archived_users=archived['A']['exposure']+archived['B']['exposure']
 return {
  'experiment_id':exp.id,'code':exp.code,'name':exp.name,
  'online':{'events':online_events,'exposures':online_exposures,'users':online_exposures},
  'archived':{'events':archived_events_count,'exposures':archived_users,'users':archived_users,'counts':archived,'batches':s.query(ArchiveBatch).filter_by(experiment_id=exp.id,state='done').count()},
  'total':{'events':online_events+archived_events_count,'exposures':online_exposures+archived_users,'users':online_exposures+archived_users},
  'latest_batch':batch_payload(latest) if latest else None,
  'can_archive':active is None and failed is None and not integrity and online_events>0,
  'can_retry':bool(failed),
  'integrity_errors':integrity,
  'failed_batch':batch_payload(failed) if failed else None,
 }
