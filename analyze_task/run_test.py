from aggerate import * 
from processdata import *
test_dir =r"D:\big_data\analyze_task\test_Data"
ensemble = aggregate_ensemble_results(test_dir)
print(ensemble)
ensemble.to_csv(os.path.join(test_dir, 'ensemble_results.csv'))
        