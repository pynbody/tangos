#!/usr/bin/env bash

get_tutorial_data() {
  if [ ! -d tutorial_$1 ]; then
    wget -O - https://zenodo.org/record/5155467/files/tutorial_$1.tar.gz?download=1 | tar -xz
  fi
}

detect_mpi() {
  if hash mpirun 2>/dev/null; then
    export MPI="mpirun -np 4"
    export MPIBACKEND="--backend=mpi4py"
    export MPILOADMODE="--load-mode=server"
    echo "Detected mpirun -- will use where appropriate"
  else
    echo "No mpirun found; running all processes in serial"
  fi
}

build_gadget4() {
  get_tutorial_data gadget4
  tangos add tutorial_gadget4
  tangos import-properties --for tutorial_gadget4
  tangos import-properties --for tutorial_gadget4 --type group
  $MPI tangos $MPIBACKEND write dm_density_profile --with-prerequisites --include-only="NDM()>5000" --type=halo --for tutorial_gadget4
}

build_gadget_subfind() {
    get_tutorial_data gadget
    tangos add tutorial_gadget --min-particles 100
    tangos import-properties --for tutorial_gadget
    tangos import-properties --type group --for tutorial_gadget
    $MPI tangos $MPIBACKEND link --for tutorial_gadget
    $MPI tangos $MPIBACKEND link --type group --for tutorial_gadget
    $MPI tangos $MPIBACKEND write dm_density_profile --with-prerequisites --include-only="NDM()>5000" --type=halo --for tutorial_gadget
}

build_gadget_rockstar() {
    get_tutorial_data gadget
    get_tutorial_data gadget_rockstar
    tangos add tutorial_gadget_rockstar --min-particles 100
    tangos import-properties Mvir Rvir X Y Z --for tutorial_gadget_rockstar
    tangos import-consistent-trees --for tutorial_gadget_rockstar
    tangos write dm_density_profile --with-prerequisites --include-only="NDM()>5000" --type=halo --for tutorial_gadget_rockstar
}

build_ramses() {
    get_tutorial_data ramses
    tangos add tutorial_ramses --min-particles 100 --no-renumber
    $MPI tangos link --for tutorial_ramses $MPIBACKEND
    $MPI tangos write contamination_fraction --for tutorial_ramses $MPIBACKEND
    $MPI tangos write dm_density_profile --with-prerequisites --include-only="contamination_fraction<0.01" --for tutorial_ramses $MPIBACKEND
}

build_changa() {
    get_tutorial_data changa$1
    tangos add tutorial_changa$1
    tangos import-properties Mvir Rvir --for tutorial_changa$1
    $MPI tangos link --for tutorial_changa$1 $MPIBACKEND
    $MPI tangos write contamination_fraction --for tutorial_changa$1 $MPIBACKEND
    $MPI tangos write dm_density_profile gas_density_profile uvi_image SFR_histogram --with-prerequisites --include-only="contamination_fraction<0.01" --include-only="NDM()>5000" $MPILOADMODE --for tutorial_changa$1  $MPIBACKEND
}

build_changa_bh() {
    build_changa _blackholes
    tangos add-bh --sims tutorial_changa_blackholes
    $MPI tangos write BH_mass BH_mdot_histogram --for tutorial_changa_blackholes --type bh $MPIBACKEND
    $MPI tangos crosslink tutorial_changa tutorial_changa_blackholes $MPIBACKEND
}

build_enzo_yt() {
  if [ -d enzo.tinycosmo ]; then
    tangos add enzo.tinycosmo --handler=yt.YtInputHandler --min-particles 100
    tangos import-consistent-trees --for enzo.tinycosmo --with-ids
    tangos import-properties Mvir Rvir X Y Z VX VY VZ --for enzo.tinycosmo
    tangos import-properties Mvir_Msun Rvir_kpc X_Mpc Y_Mpc Z_Mpc --for enzo.tinycosmo
    tangos write center center_Mpc --for enzo.tinycosmo
    $MPI tangos write Mgas Mcoldgas Mstar contamfrac --for enzo.tinycosmo $MPIBACKEND
  fi
}


echo "This script builds the tangos tutorial database"
echo
echo "It will download data and build in the current working directory:"
echo "  "`pwd`
echo
echo "The total required space is approximately 35GB"
echo
echo "If this is not what you want, press ^C now"
echo "Starting process in 5 seconds..."

sleep 5

export TANGOS_DB_CONNECTION=`pwd`/data.db
export TANGOS_SIMULATION_FOLDER=`pwd`

detect_mpi

set -e

build_gadget_subfind
build_gadget_rockstar
build_ramses
build_changa
build_changa_bh
build_gadget4
build_enzo_yt
