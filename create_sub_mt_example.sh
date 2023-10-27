#bin/bash
# Description: Create the submit script for the clustering algorithm tests
SUB_SCRIPT_DIR=/project/atlas/users/mvozak/MuonReco/submit_nikhef_batch/
RUN_SCRIPT_DIR=/project/atlas/users/mvozak/MuonReco/run/submit/test_clustering_algs/mtester/

#JOB_ID=mu_reco_sub_test_1
JOB_ID=mu_reco_sub_test_2

python ${SUB_SCRIPT_DIR}create_submit_script.py \
                    --run_type="mtester" \
                    --input_esd_dir="/dcache/atlas/muonreco/mvozak/ESD_LOCAL_OUTPUT/mu_reco_sub_test_2/" \
                    --mt_output_dir="/project/atlas/users/mvozak/MuonReco/run/submit/test_clustering_algs/mtester/${JOB_ID}" \
                    --job_id=${JOB_ID} \
                    --submit_script_name="mtester_${JOB_ID}.sh" \
                    --queue=short \
                    --fpath_output_log=${RUN_SCRIPT_DIR}/logs_${JOB_ID}/

        
