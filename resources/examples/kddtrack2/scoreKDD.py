"""
Scoring Metrics for KDD Cup 2012, Track 2

Reads in a solution/subission files

Scores on the following three metrics:
-NWMAE
-WRMSE
-AUC

Author: Ben Hamner (kdd2012@benhamner.com)
"""

def scoreElementwiseMetric(num_clicks, num_impressions, predicted_ctr, elementwise_metric):
    """
    Calculates an elementwise error metric
    
    Parameters
    ----------
    num_clicks : a list containing the number of clicks

    num_impressions : a list containing the number of impressions

    predicted_ctr : a list containing the predicted click-through rates

    elementwise_metric : a function such as MSE that evaluates the error on a single instance, given the clicks, impressions, and p_ctr
    
    Returns
    -------
    score : the error on the elementwise metric over the set
    """
    score = 0.0
    weight_sum = 0.0

    for clicks, impressions, p_ctr in zip(num_clicks, num_impressions, predicted_ctr):
        score += elementwise_metric(clicks, impressions, p_ctr)*impressions
        weight_sum += impressions
    score = score / weight_sum
    return score

def scoreWRMSE(num_clicks, num_impressions, predicted_ctr):
    """
    Calculates the Weighted Root Mean Squared Error (WRMSE)
    
    Parameters
    ----------
    num_clicks : a list containing the number of clicks

    num_impressions : a list containing the number of impressions

    predicted_ctr : a list containing the predicted click-through rates

    Returns
    -------
    wrmse : the weighted root mean squared error
    """
    import math

    mse = lambda clicks, impressions, p_ctr: math.pow(clicks/impressions-p_ctr,2.0)
    wmse = scoreElementwiseMetric(num_clicks, num_impressions, predicted_ctr, mse)
    wrmse = math.sqrt(wmse)
    return wrmse

def scoreNWMAE(num_clicks, num_impressions, predicted_ctr):
    """
    Calculates the normalized weighted mean absolute error
    
    Parameters
    ----------
    num_clicks : a list containing the number of clicks

    num_impressions : a list containing the number of impressions

    predicted_ctr : a list containing the predicted click-through rates

    Returns
    -------
    nwmae : the normalized weighted mean absolute error
    """
    mae = lambda clicks, impressions, p_ctr: abs(clicks/impressions-p_ctr)
    nwmae = scoreElementwiseMetric(num_clicks, num_impressions, predicted_ctr, mae)
    return nwmae

def scoreClickAUC(num_clicks, num_impressions, predicted_ctr):
    """
    Calculates the area under the ROC curve (AUC) for click rates
    
    Parameters
    ----------
    num_clicks : a list containing the number of clicks

    num_impressions : a list containing the number of impressions

    predicted_ctr : a list containing the predicted click-through rates

    Returns
    -------
    auc : the area under the ROC curve (AUC) for click rates
    """
    i_sorted = sorted(range(len(predicted_ctr)),key=lambda i: predicted_ctr[i],
                      reverse=True)
    auc_temp = 0.0
    click_sum = 0.0
    old_click_sum = 0.0
    no_click = 0.0
    no_click_sum = 0.0

    # treat all instances with the same predicted_ctr as coming from the
    # same bucket
    last_ctr = predicted_ctr[i_sorted[0]] + 1.0    
    #last_ctr = float("nan")

    for i in range(len(predicted_ctr)):
        if last_ctr != predicted_ctr[i_sorted[i]]: 
            auc_temp += (click_sum+old_click_sum) * no_click / 2.0        
            old_click_sum = click_sum
            no_click = 0.0
            last_ctr = predicted_ctr[i_sorted[i]]
        no_click += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]
        no_click_sum += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]
        click_sum += num_clicks[i_sorted[i]]
    auc_temp += (click_sum+old_click_sum) * no_click / 2.0
    auc = auc_temp / (click_sum * no_click_sum)
    return auc

def read_solution_file(f_sol_name):
    """
    Reads in a solution file

    Parameters
    ----------
    f_sol_name : submission file name

    Returns
    -------
    num_clicks : a list of clicks
    num_impressions : a list of impressions
    """
    f_sol = open(f_sol_name)

    num_clicks = []
    num_impressions = []
    
    i = 0
    for line in f_sol:
        line = line.strip().split(",")
        try:
            clicks = float(line[0])
            impressions = float(line[1])            
        except ValueError as e:
            # skip over header
            if(i!=0):
              print("parse error at line: %d" % i)
              print(e)
            continue
        num_clicks.append(clicks)
        num_impressions.append(impressions)
        i += 1
    print("submission length=%d" % i)
    return (num_clicks, num_impressions)

def read_submission_file(f_sub_name):
    """
    Reads in a submission file 

    Parameters
    ----------
    f_sub_name : submission file name

    Returns
    -------
    predicted_ctr : a list of predicted click-through rates
    """
    f_sub = open(f_sub_name)

    predicted_ctr = []

    for line in f_sub:
        line = line.strip().split(",")
        predicted_ctr.append(float(line[0]))
        #predicted_ctr.append(float(line))

    return predicted_ctr

def main():
    import sys
    if len(sys.argv) != 3:
        print("Usage: python scoreKDD.py solution_file.csv submission_file.csv")
        sys.exit(2)

    num_clicks, num_impressions = read_solution_file(sys.argv[1])
    predicted_ctr = read_submission_file(sys.argv[2])

    print("num_clicks : %d" % len(num_clicks))
    print("num_impressions : %d" % len(num_impressions))
    print("num_predicted_ctrs: %d" % len(predicted_ctr))

    auc = scoreClickAUC(num_clicks, num_impressions, predicted_ctr)
    print("AUC  : %f" % auc)
    nwmae = scoreNWMAE(num_clicks, num_impressions, predicted_ctr)
    print("NWMAE: %f" % nwmae)
    wrmse = scoreWRMSE(num_clicks, num_impressions, predicted_ctr)
    print("WRMSE: %f" % wrmse)

if __name__=="__main__":
    main()
