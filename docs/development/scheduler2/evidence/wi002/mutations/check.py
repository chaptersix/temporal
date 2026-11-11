import hashlib, json, os, subprocess
from pathlib import Path
root=Path.cwd()
out=Path('/private/tmp/scheduler2-wi002-final/mutations'); out.mkdir(parents=True,exist_ok=True)
core=Path('chasm/lib/scheduler2/internal/occurrence/occurrence.go')
source=Path('service/worker/scheduler/candidate.go')
cases=[
 ('lost-pending',core,'result.Status = Waiting','result.State.DisposedThrough = pending.Nominal\n\t\t\t\tresult.State.Pending = nil\n\t\t\t\tresult.Status = Waiting','TestSavedPendingJitterReplay'),
 ('yield-as-exhaustion',core,'result.Status = Yield','result.Status = Exhausted','TestExcludedWorkYieldsWithoutExhaustion'),
 ('unbounded-jitter',source,'maxJitter = min(maxJitter, following.Sub(nominal))','maxJitter = timestamp.DurationValue(cs.spec.Jitter)','TestBoundedEnumerationAgainstScan'),
]
manifest={'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'results':[]}
for name,path,old,new,test in cases:
 original=path.read_bytes(); text=original.decode(); assert text.count(old)==1
 mutated=text.replace(old,new).encode()
 command=['go','test','-p','2','-parallel','4','-tags','test_dep','-count=1','-timeout=2m','./chasm/lib/scheduler2/internal/occurrence','-run','^'+test+'$','-v']
 try:
  path.write_bytes(mutated)
  with (out/(name+'.log')).open('w') as log:
   result=subprocess.run(command,env=dict(os.environ,CGO_ENABLED='0',GOMAXPROCS='4'),stdout=log,stderr=subprocess.STDOUT,timeout=150)
  log=(out/(name+'.log')).read_text()
  caught=result.returncode==1 and '--- FAIL: '+test in log and '[build failed]' not in log
  manifest['results'].append({'mutation':name,'path':str(path),'original_sha256':hashlib.sha256(original).hexdigest(),'mutant_sha256':hashlib.sha256(mutated).hexdigest(),'old':old,'new':new,'command':command,'exit_code':result.returncode,'detected':caught})
 finally:
  path.write_bytes(original)
 assert path.read_bytes()==original
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(manifest,indent=2))
assert all(r['detected'] for r in manifest['results'])
