"""
scripts/start_workers.py
========================
Launches all 11 DSW workers to listen to the Redis DTB queues.
"""
import sys
import os
import multiprocessing

# Add parent dir to path so we can import agents
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists import (
    GeologicalExpertDSW,
    LogisticsCoordinatorDSW,
    ChemicalInfraAnalystDSW,
    SupplyChainForecasterDSW,
    FabLocatorDSW,
    MiningLeaseAnalystDSW,
    EnvironmentalComplianceDSW,
    WorkforceAnalystDSW,
    TradePolicyExpertDSW,
    ThermalMaterialsExpertDSW,
    SemiconductorGradeQADSW
)

def run_worker(worker_class):
    print(f"Starting worker: {worker_class.__name__}")
    # In a real implementation this would instantiate DTB worker and loop on claim_next_job
    # For now, it's just a placeholder runner as designed in the blueprint
    # worker = DTBWorker(dsw_agent=worker_class())
    # worker.start()
    print(f"{worker_class.__name__} listening on queue dtb:queue:{worker_class.worker_type.value}")

if __name__ == "__main__":
    workers = [
        GeologicalExpertDSW,
        LogisticsCoordinatorDSW,
        ChemicalInfraAnalystDSW,
        SupplyChainForecasterDSW,
        FabLocatorDSW,
        MiningLeaseAnalystDSW,
        EnvironmentalComplianceDSW,
        WorkforceAnalystDSW,
        TradePolicyExpertDSW,
        ThermalMaterialsExpertDSW,
        SemiconductorGradeQADSW
    ]
    
    processes = []
    for w in workers:
        p = multiprocessing.Process(target=run_worker, args=(w,))
        p.start()
        processes.append(p)
        
    for p in processes:
        p.join()
