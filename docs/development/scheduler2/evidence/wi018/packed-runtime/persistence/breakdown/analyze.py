import sys,gzip,base64,collections,json

def var(b,p):
 n=0;s=0
 while True:
  x=b[p];p+=1;n|=(x&127)<<s
  if x<128:return n,p
  s+=7

def blob(b,p):
 n,p=var(b,p);return b[p:p+n],p+n

def fields(b):
 p=0;out=[]
 while p<len(b):
  start=p;tag,p=var(b,p);typ=tag&7
  if typ==2:v,p=blob(b,p)
  elif typ==0:v,p=var(b,p)
  elif typ==1:v=b[p:p+8];p+=8
  elif typ==5:v=b[p:p+4];p+=4
  else:raise ValueError(typ)
  out.append((tag>>3,v,b[start:p]))
 return out

def get(b,n):return next((v for k,v,_ in fields(b) if k==n),b'')
def expand(template,wire):
 if wire[0]==0:return wire[1:]
 assert wire[0]==1
 out=b'';p=1
 for _,_,f in fields(template):
  pre,p=var(wire,p);suf,p=var(wire,p);mid,p=blob(wire,p)
  out+=f[:pre]+mid+(f[len(f)-suf:] if suf else b'')
 assert p==len(wire)
 return out

def pathdecode(s):
 out='';esc=False
 for c in s:
  if esc:out+=c;esc=False
  elif c=='\\':esc=True
  else:out+=c
 return out

wire=gzip.open(sys.argv[1],'rb').read();pages={};requests={};groups=collections.defaultdict(lambda:collections.Counter());leaf=collections.defaultdict(list)
for k,v,_ in fields(wire):
 if k!=12:continue
 path=get(v,1).decode();node=get(v,2);group=path.split('#')[0];groups[group].update(nodes=1,node_bytes=len(node),path_bytes=len(path))
 data=get(get(node,2),2)
 if group=='PackedPages' and '#' in path:pages[pathdecode(path.split('#',1)[1])]=get(data,2)
 if group=='PackedRequests' and '#' in path:requests[path.split('#',1)[1]]=get(data,1)
stats=collections.defaultdict(lambda:collections.Counter())
for key,b in pages.items():
 if not key.startswith('p/'):continue
 _,ns,ordinal=key.split('/');ns=base64.urlsafe_b64decode(ns+'='*((-len(ns))%4)).decode();st=stats[ns];st.update(pages=1,encoded_bytes=len(b))
 if ordinal=='0':continue
 assert b[0]==1 and b[1] in(0,1) and 0<b[2]<=16
 count=b[2];p=3;prefix,p=blob(b,p)
 for i in range(count):
  suffix,p=blob(b,p);ek=prefix+suffix
  if b[1]:
   ev,p=blob(b,p);leaf[ns].append((ek,ev));st.update(records=1,logical_key_bytes=len(ek),stored_key_suffix_bytes=len(suffix),value_bytes=len(ev))
  else:
   child,p=var(b,p);cnt,p=var(b,p)
 assert p==len(b)
qfields=collections.defaultdict(lambda:collections.Counter());template=pages['t/Q'];qsize=0
for q in requests.values():
 original=expand(template,q);qsize+=len(q);base=fields(template);curr=fields(original);p=1
 assert q[0]==1
 for (tag,_,bf),(_,_,cf) in zip(base,curr):
  start=p;pre,p=var(q,p);suf,p=var(q,p);mid,p=blob(q,p)
  qfields[str(tag)].update(raw_bytes=len(cf),delta_bytes=p-start,literal_bytes=len(mid))
result={'file':sys.argv[1],'total_proto_bytes':len(wire),'groups':groups,'namespaces':stats,'request_count':len(requests),'request_delta_bytes':qsize,'request_template_bytes':len(template),'request_fields':qfields}
print(json.dumps(result,indent=2,sort_keys=True))
import zlib
candidate={}
for ns,items in list(leaf.items())+[('Q',[(b'',q) for q in requests.values()])]:
 if 't/'+ns not in pages:continue
 template=pages['t/'+ns];old=0;new=0;rawlen=0
 for _,delta in items:
  original=expand(template,delta);old+=len(delta);rawlen+=len(original)
  enc=zlib.compressobj(1,zlib.DEFLATED,-15,zdict=template);compressed=enc.compress(original)+enc.flush();dec=zlib.decompressobj(-15,zdict=template);assert dec.decompress(compressed)+dec.flush()==original
  new+=min(len(delta),len(compressed)+1)
 candidate[ns]={'old_value_bytes':old,'dictionary_deflate1_candidate_bytes':new,'raw_bytes':rawlen,'saved':old-new}
print('CANDIDATE',json.dumps(candidate,sort_keys=True))
