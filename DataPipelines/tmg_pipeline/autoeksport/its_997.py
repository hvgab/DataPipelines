from autoeksport import Autoeksport, TmgServer
import settings
import pandas as pd

def get_all_jobs_in_project(project, conn):
    query = f"""SELECT project, job, concat(project, '/', job) as projectjob, state 
                FROM tmg_config.job 
                WHERE project = '{project}' AND state not in ('deleted')"""
    df = pd.read_sql(query, conn)
    return df

if __name__ == '__main__':

    DROP_COLS = [
        #  JC
        # 'projectJob', 'id', 'rejectReason', 'endReason', 
        # '_numberOfSmsSent', 'smsOrderState', 'smsWorkflowOutcome', 'smsWorkflowState',
        #  SESSION
        # 'sessionId', 'state', 'sessionUpdated', 'saleClosed', 'saleConfirmed',
        # 'saleConfirmedMethod', 'saleCancelled', 'saleCancelledBy', 'lastCallState', 'contactResponse',
        #  PROD
        'productId', 'productName', 'freight', 'unitPriceToCustomer', 'fixedPrice', 'discountInPercent', 'vat', 'quantum', 'salesAmount', 'deriveSalesAmount', 'displayedTextToAgent', 'writtenToFileValue',
    ]
    
    PROJECTS = [
        'Telia', 
        'Telia_Getit',
        'OneCall'
    ]
    
    TMG_SERVER = TmgServer(**settings.tmg_servers["main"])
    conn = f'mysql+pymysql://{TMG_SERVER.username}:{TMG_SERVER.password}@{TMG_SERVER.host}'
    
    for project in PROJECTS:
        print(f'project: {project}')
        # Get all jobs in project
        jobs = get_all_jobs_in_project(project, conn)
        
        for row in jobs.itertuples():
            print('row:')
            print(row)
            print(row.projectjob)
            # print(row['projectjob'])
        
            export = Autoeksport(TMG_SERVER,
                             projectJobs=[row.projectjob],
                             exportfields='basic',
                             add_prefix=False,
                             drop_cols=DROP_COLS)
    
            export.main_export()
    
