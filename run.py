import numpy as np
import re
import glob

from icecube.icetray import I3Tray
from i3_to_parquet import I3ToParquetModule_BySensor

muon_files = sorted(glob.glob('/n/holylfs05/LABS/arguelles_delgado_lab/Lab/IceCube_MC/21217/0000000-0000999/*.i3.zst'))[80:100]
e_files = sorted(glob.glob('/n/holylfs05/LABS/arguelles_delgado_lab/Lab/IceCube_MC/21218/0000000-0000999/*.i3.zst'))[80:100]
tau_files = sorted(glob.glob('/n/holylfs05/LABS/arguelles_delgado_lab/Lab/IceCube_MC/21219/0000000-0000999/*.i3.zst'))[80:100]

infiles = muon_files + e_files + tau_files

for infile in infiles:
    
    pattern = r'(\d+\.\d+)(?=\.i3\.zst$)'
    match = re.search(pattern, infile)
    output_file_name = match.group(1) if match else None
    
    tray = I3Tray()

    tray.Add('I3Reader', FilenameList = [infile])
    tray.AddModule(I3ToParquetModule_BySensor, 'I3ToParquetModule_BySensor', outfile = '/n/holylfs05/LABS/arguelles_delgado_lab/Everyone/felixyu/IceCube_MC/test/' + output_file_name + '.parquet')

    tray.Execute()