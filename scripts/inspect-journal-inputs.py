"""Report exact synthetic corpus counts from checksum-verified physical frames."""
import argparse,collections,hashlib,json,struct
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--inputs',required=True);p.add_argument('--out',required=True);args=p.parse_args();directory=Path(args.inputs);report={}
 for file in sorted(directory.glob('*.journal')):
  data=file.read_bytes();off=0;previous='';store=None;kinds=collections.Counter();requests=set();sessions=set();records=0;trees=set();bindings=set()
  while off<len(data):
   if data[off:off+8]!=b'FRTOP001' or len(data)-off<48:raise RuntimeError('invalid framing')
   length=struct.unpack('>Q',data[off+8:off+16])[0];end=off+16+length
   if end+32>len(data):raise RuntimeError('truncated frame')
   digest=hashlib.sha256(b'forest-topology/record/v1\0'+data[off:end]).digest()
   if digest!=data[end:end+32]:raise RuntimeError('bad physical checksum')
   r=json.loads(data[off+16:end]);records+=1
   if r['sequence']!=records or r['previous_digest']!=previous:raise RuntimeError('bad chain')
   if store is not None and r['store_id']!=store:raise RuntimeError('mixed store')
   store=r['store_id'];previous=digest.hex();kinds[r['kind']]+=1
   if r.get('request'):requests.add(r['request']['id'])
   if r.get('session_id'):sessions.add(r['session_id'])
   if r.get('intent'):
    requests.add(r['intent']['request']['id']);sessions.add(r['intent']['session_id'])
   m=r.get('target') or (r.get('intent') or {}).get('target')
   if m:
    trees.update(t['id'] for t in m['trees']);bindings.update(b['id'] for b in m['bindings'])
   off=end+32
  report[file.stem]=dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),records=records,requests=len(requests),sessions=len(sessions),record_kinds=dict(kinds),unique_tree_ids=len(trees),unique_binding_ids=len(bindings),head_digest=previous)
 Path(args.out).write_text(json.dumps(report,indent=2),encoding='utf-8');print('verified corpus count:',len(report))
if __name__=='__main__':main()
