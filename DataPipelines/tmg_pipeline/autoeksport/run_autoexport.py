from autoeksport import Autoeksport, TmgServer
import settings

if __name__ == '__main__':

    DROP_COLS = ['projectJob', 'id', 'rejectReason', 'endReason', 
                 #  '_numberOfSmsSent', 'smsOrderState', 'smsWorkflowOutcome', 'smsWorkflowState',
                 'sessionId', 'state', 'sessionUpdated', 'saleClosed', 'saleConfirmed',
                 'saleConfirmedMethod', 'saleCancelled', 'saleCancelledBy', 'lastCallState', 'contactResponse',
                 #  Prod:
                 'productId', 'productName', 'freight', 'unitPriceToCustomer', 'fixedPrice', 'discountInPercent', 'vat', 'quantum', 'salesAmount', 'deriveSalesAmount', 'displayedTextToAgent', 'writtenToFileValue',
                 ]
    
    PROJECTJOBS = [
        'X/Y'
    ]
        
    TMG_SERVER = TmgServer(**settings.tmg_servers["main"])
    
    for pj in PROJECTJOBS:
        # ITS-997
        export = 
        
        # Ringbare
        # export = Autoeksport(TMG_SERVER,
        #                      projectJobs=[pj], 
        #                      exportfields='basic', 
        #                      add_prefix=False, 
        #                      session_state_in=['open'], 
        #                      dialplan_contactresponse_in='NULL', 
        #                      drop_cols=None)


        # Responsfil / Total eksport?
        # export = Autoeksport(TMG_SERVER,
        #                      projectJobs=[pj], 
        #                      exportfields='basic',
        #                      add_prefix=False,
        #                      session_state_in=None,
        #                      dialplan_contactresponse_in=None,
        #                      drop_cols=None)
        
        # Salg
        # export = Autoeksport(projectJobs=[pj], 
        #                     exportfields='basic',
        #                     add_prefix=False,
        #                     session_state_in=['confirmed'],
        #                     dialplan_contactresponse_in=None,
        #                     drop_cols=None)
    
        export.main_export()
    
