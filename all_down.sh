#!/usr/local/bin/bash

#MAIN FILES (edit these for customisation)


main_dir=$(dirname $0) #comment out if using alternative
#main_dir='/Users/mrosello' #comment out if usin same directory as this script
main_log="${main_dir}/main_log.txt"
#database shell: (should already exist at run time)
empty_database="${main_dir}/EbiSample.db"
#python runnables
runnable_fillsample="${main_dir}/fillsamples.py"
fillsamplelogfile="${main_dir}/fillsamples.log"
#log files for python runnables
runnable_studydump="${main_dir}/dumpsamples.py"
studydumplogfile="${main_dir}/dumpsamples.log"
#ENA API dumps for plant samples (xml), plant studies (json), analysis (ERZ) data (json)
#wget logs are saved, but these are removed later
ENAsamp="${main_dir}/all_samp.xml"
ENAsamp_log="${ENAsamp}.log"
ENAsampTries=6 #how many times to try sample download (usually needs multiple attempts)
ENAanal="${main_dir}/nolimit_analysis.json"
ENAanal_log="${ENAanal}.log"
ENAstud="${main_dir}/allstud.json"
#json file dumps (they are put in main_dir before being copied to ftp_dir afterwards)
ftp_dir='/nfs/ensemblgenomes/ftp/pub/misc_data/plant_index'
#ftp://ftp.ensemblgenomes.org/pub/misc_data/plant_index/
studyjson="${main_dir}/study_dump.json"
germplasmjson="${main_dir}/gp_dump.json"

#TEMP FILES  probably no need for customisation.

#  these are removed at the end but can keep them for debugging if required
#  ENA API dumps for plant studies done in 3 steps to capture majority (then merged to $ENAstud) 
#    result:study
res_study="${main_dir}/res_study.json"
res_study_log="${res_study}.log"
#    result:read_study
res_read_study="${main_dir}/res_read_study.json"
res_read_study_log="${res_read_study}.log"
#    result:analysis_study
res_anal_study="${main_dir}/res_anal_study.json"
res_anal_study_log="${res_anal_study}.log" 

#copy empty db so that the original can be reused during multiple runs
database="${main_dir}/EbiSample_content.db"
cp $empty_database $database

#WGET files required for filling $database


#  XML file containing plant samples from ENA webservice/REST
echo "step 1: wget of plant samples in ENA using REST URL:" | tee -a $main_log
echo "$get_all_ena_plant_samples" | tee -a $main_log
timestart=$(date '+%s')
get_all_ena_plant_samples='https://www.ebi.ac.uk/ena/data/view/Taxon:33090&portal=sample&subtree=true&display=xml'

for i in {1..$ENAsampTries}
do
    wget -O $ENAsamp $get_all_ena_plant_samples >& $ENAsamp_log
    eof=$(tail -c 7 $ENAsamp) #last 7 char = </ROOT>
    if [[ "$eof" != "</ROOT>" ]] #very basic check if download worked
    then 
	if [ $i -lt $ENAsampTries ]; then
	    rm $ENAsamp
	    echo "tried wget of plant samples $i times" | tee -a $main_log
	    continue
	fi
	echo "may have been a problem getting samples (looking for file ending with '</ROOT>') after $i tries" | tee -a $main_log
	echo "file:$ENAsamp" | tee -a $main_log
	echo "check wget log:$ENAsamp_log" | tee -a $main_log
	exit
    else
	break
    fi
done


rm $ENAsamp_log 
newtime=$(date '+%s')
seconds=$(echo $newtime - $timestart | bc)
totalsamp=$(grep -c '</SAMPLE>' $ENAsamp) #useful count for later too
echo "completed step 1: wget of plant samples in ENA" | tee -a $main_log
echo "took $seconds seconds" | tee -a $main_log
echo "counted $totalsamp samples in XML file" | tee -a $main_log
sed -i '/^Entry:/d' $ENAsamp # i noticed these lines which are not in xml format. will break parsing if not removed


#  JSON file containing 'analysis' data (variant files, alignment files etc ..) connected to plant samples 
echo "Step 2: wget of analysis objects attached to plant samples in ENA using REST URL:${get_all_plant_anal}" | tee -a $main_log
timenow=$(date '+%s')
get_all_plant_anal='https://www.ebi.ac.uk/ena/portal/api/search?result=analysis&query=tax_tree(33090)&fields=sample_accession%2Csubmitted_ftp%2Csubmitted_md5%2Cstudy_accession%2Canalysis_type%2Ccenter_name%2Cbroker_name%2Canalysis_title&limit=0&format=json'
wget -O $ENAanal $get_all_plant_anal >& $ENAanal_log
eof=$(tail -c 4 $ENAanal) #last 4 char = }] (plus a couple of newlines)
if [[ $eof != }*]* ]]
then
    echo "may be a problem getting analysis objects attached to plant samples" | tee -a $main_log
    echo "file:${ENAanal}" | tee -a $main_log
    echo "check wget log:${ENAanal_log}" | tee -a $main_log
    exit
fi
rm $ENAanal_log
newtime=$(date '+%s')
seconds=$(echo $newtime - $timenow | bc)
totalanal=$(grep -c "analysis_accession" $ENAanal)
echo "completed step 2: wget of analysis objects attached to plant samples in ENA" | tee -a $main_log
echo "took $seconds seconds. Downloaded ${totalanal} analysis objects in ${ENAanal}" | tee -a $main_log


#  3 JSON files to find all studies associated to plant samples
echo "Step 3: wget of studies using 3 API calls for (slightly) different results and then merging them" | tee -a $main_log
timenow=$(date '+%s')
get_res_study='https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=tax_tree(33090)&fields=study_accession%2C%20secondary_study_accession%2C%20study_description%2Cgeo_accession%2C%20study_name%2C%20study_title%2C%20breed%2C%20cultivar%2C%20isolate&format=json'
wget -O $res_study $get_res_study >& $res_study_log
get_res_read_study='https://www.ebi.ac.uk/ena/portal/api/search?result=read_study&query=tax_tree(33090)&fields=study_accession%2Csecondary_study_accession%2Cstudy_title%2Cstudy_alias&format=json'
wget -O $res_read_study $get_res_read_study >& $res_read_study_log
get_res_anal_study='https://www.ebi.ac.uk/ena/portal/api/search?result=analysis_study&query=tax_tree(33090)&fields=study_accession%2Csecondary_study_accession%2Cstudy_title%2Cstudy_alias&format=json'
wget -O $res_anal_study $get_res_anal_study >& $res_anal_study_log
eof=$(tail -c4 $res_study $res_read_study $res_anal_study | sed -e '/^$/d' -e '/==/d')
if [[ $eof != }*]*}*]*}*]* ]] 
then
    echo "may have been a problem getting 1 of the 3 study API results." | tee -a $main_log
    echo "Files:" | tee -a $main_log
    echo $res_study | tee -a $main_log
    echo $res_read_study | tee -a $main_log
    echo $res_anal_study | tee -a $main_log
    echo "check wget logs:" | tee -a $main_log
    echo $res_study_log | tee -a $main_log
    echo $res_read_study_log | tee -a $main_log
    echo $res_anal_study_log | tee -a $main_log
    exit
fi
rm $res_study_log $res_read_study_log $res_anal_study_log
newtime=$(date '+%s')
seconds=$(echo $newtime - $timenow | bc)
echo "completed wget of studies using 3 API calls for (slightly) different results in ${seconds} seconds. Used REST URLs:" | tee -a $main_log
echo "${get_res_study}" | tee -a $main_log
echo "${get_res_read_study}" | tee -a $main_log
echo "${get_res_anal_study}" | tee -a $main_log
echo "will try to merge them into 1 ..." | tee -a $main_log


#   Merge 3 JSON files to one
fcoma=$(mktemp)
echo ',' > $fcoma
head -n -1 $res_study > ${res_study}_temp
head -n -1 $res_read_study | tail -n +2 > ${res_read_study}_temp
tail -n +2 $res_anal_study > ${res_anal_study}_temp
cat ${res_study}_temp $fcoma ${res_read_study}_temp $fcoma ${res_anal_study}_temp > $ENAstud
rm ${res_study}_temp ${res_read_study}_temp ${res_anal_study}_temp $fcoma
echo "Step 3 completed. created file ${ENAstud}" | tee -a $main_log
echo "This file will have duplicates but they will be removed at the next stage" | tee -a $main_log


#STAGE TWO (makesure db shell is present in main dir)
#fillsample python class ($runnable_fillsample) takes the files created above and populates $database
echo "Step 4: running ${runnable_fillsample} to populate ${database}" | tee -a $main_log
timenow=$(date '+%s')
$runnable_fillsample $database $ENAsamp $ENAanal $ENAstud $fillsamplelogfile
problem=$?
if [ $problem -ne 0 ] 
then
    echo "problem encountered while running ${runnable_fillsample} check log at ${fillsamplelogfile} and sterr" | tee -a $main_log
    exit
fi
newtime=$(date '+%s')
seconds=$(echo $newtime - $timenow | bc)
echo "completed step 4: ${runnable_fillsample} in ${seconds} seconds" | tee -a $main_log

#STAGE THREE
#dumpsamples python class ($runnable_studydump) parsed the $database and creates JSON of studies and JSON of germplasms
baseS=$(basename $studyjson)
baseG=$(basename $germplasmjson)
echo "Step 5: running ${runnable_studydump} to parse ${database} and create files ${baseS} and ${baseG}" | tee -a $main_log
timenow=$(date '+%s')
$runnable_studydump $database $studyjson $germplasmjson $studydumplogfile
problem=$?
if [ $problem -ne 0 ] 
then
    echo "problem encountered while running ${runnable_studydump} check log at ${studydumplogfile} and sterr" | tee -a $main_log
    exit
fi
newtime=$(date '+%s')
seconds=$(echo $newtime - $timenow | bc)
echo "completed step 5: ${runnable_studydump} in ${seconds} seconds" | tee -a $main_log
echo "moving ${baseS} and ${baseG} to ftp directory: ${ftp_dir}" | tee -a $main_log
chmod o=r $studyjson $germplasmjson
cp $studyjson $ftp_dir
cp $germplasmjson $ftp_dir
today=$(date +"%b-%d-%y")
seconds=$(echo $newtime - $timestart | bc)
readme="${ftp_dir}/readme.txt"
echo "files ${baseS} and ${baseG} last dumped on ${today}" > $readme
days=$(echo "$seconds / (60 * 60 * 24)" | bc)
seconds=$(echo "$seconds - $days * (60 * 60 * 24)" | bc)
hours=$(echo "$seconds / (60 * 60)" | bc)
seconds=$(echo "$seconds - $hours * (60 * 60)" | bc)
minutes=$(echo "$seconds / 60" | bc)
echo "pipeline took $days days $hours hours and $minutes minutes" | tee -a $readme $main_log
echo "find pipeline here: https://github.com/EnsemblGenomes/ebi_plant_index" | tee -a $readme $main_log
chmod o=r $readme

echo "**completed**" | tee -a $main_log
echo "if all is well you can remove the following:" | tee -a $main_log
echo "$database" | tee -a $main_log
echo "$fillsamplelogfile" | tee -a $main_log
echo "$studydumplogfile" | tee -a $main_log
echo "$ENAsamp" | tee -a $main_log
echo "$ENAanal" | tee -a $main_log
echo "$ENAstud" | tee -a $main_log
echo "$studyjson" | tee -a $main_log
echo "$germplasmjson" | tee -a $main_log


