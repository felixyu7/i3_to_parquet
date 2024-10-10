import awkward as ak
import numpy as np

from icecube import icetray, dataio
from icecube.icetray import I3Tray
from icecube.icetray import I3Frame as F
from icecube.icetray import I3Module as M
from icecube import dataclasses as dc
from icecube.dataclasses import I3Particle as P
from icecube.dataclasses import I3MCTree as T
from icecube.dataclasses import I3ParticleID as PID

def aggregate_lists(list1, list2):
    # Initialize a dictionary to hold the aggregated results
    aggregation = {}
    
    # Iterate over both lists simultaneously
    for item1, item2 in zip(list1, list2):
        # Create a list for each unique item in list2, if not already created
        if item2 not in aggregation:
            aggregation[item2] = []
        # Append the corresponding item from list1 into the list for the key item2
        aggregation[item2].append(item1)
    
    # Extract unique keys from list2
    unique_values_list2 = list(aggregation.keys())
    
    # Extract the aggregated results into a list of lists
    aggregated_list1 = list(aggregation.values())
    
    # Return the results
    return unique_values_list2, aggregated_list1

def convert_omkey(omkey):
    return 60 * (omkey.string - 1) + (omkey.om - 1)

class I3ToParquetModule_BySensor(M):
    def __init__(self, context): # in this case the context should be a file path
        M.__init__(self, context)
        self.AddParameter("outfile", "output file save path", None)
        
    def Configure(self):
        # initialize some global parameters
        self.pulse_series = []
        self.pulse_counter = 0
        self.event_counter = 0
        
        self.outfile = self.GetParameter("outfile")
        
    def Physics(self, frame):
        # check if in split physics frame
        if 'SplitInIceDSTPulses' not in frame:
            return
        
        pulse_map = dc.I3RecoPulseSeriesMap.from_frame(frame, 'SplitInIceDSTPulses')
        
        # taken conveniently from Philip
        om_ids = []
        pulse_times = []
        pulse_charges = []
        aux = []
        for omkey, pulse_vector in pulse_map.items():
            if omkey.om > 60:
                continue
            om_id = convert_omkey(omkey)
            
            for p in pulse_vector:
                om_ids.append(om_id)
                pulse_times.append(p.time)
                pulse_charges.append(p.charge)
                if p.flags % 2 != 0:  # LC
                    aux.append(False)
                else:  # not LC
                    aux.append(True)
        
        # Sort pulses
        om_ids = [x for _, x in sorted(zip(pulse_times, om_ids))]
        pulse_charges = [x for _, x in sorted(zip(pulse_times, pulse_charges))]
        aux = [x for _, x in sorted(zip(pulse_times, aux))]
        
        event_id = frame['I3EventHeader'].event_id
        event_ids = [event_id] * len(om_ids)
        
        # shift time by time range start
        start_time = frame['SplitInIceDSTPulsesTimeRange'].start
        pulse_times = [t - start_time for t in pulse_times]
        
        unique_om_ids, agg_pulse_times = aggregate_lists(pulse_times, om_ids)
        _, agg_pulse_charges = aggregate_lists(pulse_charges, om_ids)
        _, agg_aux = aggregate_lists(aux, om_ids)

        pulse_info = []
        for i in range(len(unique_om_ids)):
            pulse_info.append({
                'event_id': event_id,
                'om_id': unique_om_ids[i],
                'pulse_times': agg_pulse_times[i],
                'pulse_charges': agg_pulse_charges[i],
                'aux': agg_aux[i]
            })
        
        self.pulse_series.extend(pulse_info)
        self.event_counter += 1
        self.pulse_counter += len(unique_om_ids)
        
    def Finish(self):
        data = ak.Array([{'event_count': self.event_counter,
                         'pulse_count': self.pulse_counter,
                         'pulse_info': self.pulse_series}])
        ak.to_parquet(data, self.outfile)
        
    
        
        