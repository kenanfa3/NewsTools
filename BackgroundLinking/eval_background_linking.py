import subprocess
treceval = "/home/kenanfayoumi/trec_eval/trec_eval"
# qrels = "/DATA/projects/TRECNews/2018/bl/bqrels.exp-gains.txt"
# qrels = "/DATA/projects/TRECNews/2019/bl/newsir19-qrels-background.txt"
# qrels = "/DATA/projects/TRECNews/2020/bl/qrels.background"
qrels = "/DATA/projects/TRECNews/2021/bl/qrels.background"

run_file = "/DATA/users/kenanfayoumi/BackgroundLinking/res_files/finetuned/FINETUNED_2018_2019_2020_TEST2018"
run_file = "/DATA/users/kenanfayoumi/BackgroundLinking/res_files/wapoV4_BASELINE_2021"

result = subprocess.run([treceval,
                         '-M 100',
                         '-q',
                         '-mall_trec',
                         # '-mndcg.1=2,2=4,3=8,4=16',
                         # '-l16',
                         qrels,
                         run_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print( 'exit status:', result.returncode )
print( 'stdout:', result.stdout.decode() )
print( 'stderr:', result.stderr.decode() )