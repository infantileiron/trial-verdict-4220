import os,time,json,logging
from pathlib import Path
import redis
from src.config.database import Session
from src.models.entities import Decision,ArchiveBatch
from src.services.analysis import calculate
from src.services.archive_jobs import process as process_archive
r=redis.Redis.from_url(os.environ['REDIS_URL'])
with Session() as recovery:
 for pending in recovery.query(Decision).filter(Decision.state.in_(['queued','running'])).all():pending.state='queued';r.rpush('trial:decisions',pending.id)
 for pending in recovery.query(ArchiveBatch).filter(ArchiveBatch.state.in_(['queued','migrating'])).all():
  pending.state='queued';r.rpush('trial:archives',pending.id)
 recovery.commit()
while True:
 Path('/tmp/heartbeat').touch()
 try:
  item=r.blpop(['trial:decisions','trial:archives'],timeout=3)
  if not item:continue
  queue,raw=item
  if queue.decode()=='trial:archives':
   with Session() as s:
    row=s.get(ArchiveBatch,int(raw))
    if not row or row.state!='queued':continue
    process_archive(s,row)
   continue
  with Session() as s:
   row=s.get(Decision,int(raw))
   if not row or row.state!='queued':continue
   row.state='running';s.commit()
   try:
    row.result=calculate(row.snapshot);row.state='done'
    Path('/artifacts/trial-verdict-'+str(row.id)+'.json').write_text(json.dumps(row.result,ensure_ascii=False))
   except Exception as e:row.state='failed';row.error=str(e)
   s.commit()
 except Exception:logging.exception('worker');time.sleep(2)
