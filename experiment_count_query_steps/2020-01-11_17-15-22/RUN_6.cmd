#!/bin/bash
#SBATCH -n 5 
#SBATCH --mem=17000 
#SBATCH -t 300 
#SBATCH --output 2020-01-11_17-15-22/main_server_multiprog6_k_1024,2048,4096,8192,16384,32768_n_16,32,64,128,256_i_0,1.out 

srun --wait 0 --multi-prog 2020-01-11_17-15-22/main_server_multiprog6_k_1024,2048,4096,8192,16384,32768_n_16,32,64,128,256_i_0,1.conf
