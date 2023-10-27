#bin/bash
# Description: Create the submit script for the clustering algorithm tests
SUB_SCRIPT_DIR=/project/atlas/users/mvozak/MuonReco/submit_nikhef_batch/
RUN_SCRIPT_DIR=/project/atlas/users/mvozak/MuonReco/run/submit/test_clustering_algs/

JOB_ID=mu_reco_sub_test_2

python ${SUB_SCRIPT_DIR}create_submit_script.py \
                    --fpath_run_script=${RUN_SCRIPT_DIR}/run_template.sh \
                    --input_hits_dir="/dcache/atlas/muonreco/mvozak/HITS/PgunDiMu_Pt10to100R3/" \
                    --job_id=${JOB_ID} \
                    --submit_script_name=${JOB_ID}.sh \
                    --split_into_chunks=True\
                    --queue=short \
                    --chunk_size 1000 \
                    --fpath_output_log=${RUN_SCRIPT_DIR}/logs_${JOB_ID}/

        
