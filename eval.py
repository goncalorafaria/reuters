import math

def eval(qtrain, pred, beta=0.5):
    tkeys = list(pred.keys())
    results = {}
    for topic in tkeys:
        y_true = qtrain[topic]
        answers = pred[topic]

        precision, recall, fbeta = classic_metrics(answers, y_true, beta=beta)

        entry = {}
        entry["precision"] = precision
        entry["recall"] = recall
        entry["fbeta"] = fbeta
        entry["bref"] = bref(answers,y_true)
        entry["mrr"] = reciprocal_rank(answers,y_true)
        entry["ndcg"] = dcg(answers,y_true)
        entry["map"] = ap(answers,y_true)
        results[topic] = entry


    return results, ["precision", "recall","fbeta","bref", "mrr","ndcg","map"]

def classic_metrics( answers, y_true, beta=0.5):

    judged_relevant = set()
    judged_nonrelevant = set()

    for d,bo in y_true:
        judged_relevant.add(d) if bo else judged_nonrelevant.add(d)

    tp = len(answers.intersection(judged_relevant))

    judged_and_retrieved = (len(answers.intersection(judged_relevant))+len( answers.intersection(judged_nonrelevant)))

    if judged_and_retrieved == 0:
        return 0.0,0.0,0.0

    precision = float(tp) / judged_and_retrieved

    recall = float(tp) / len(judged_relevant)

    if tp == 0:
        fbeta = 0.0
    else:
        fbeta = (1 + beta**2) * precision * recall/(beta**2 * precision + recall)

    return precision, recall, fbeta

def bref(pred,y_true):

    relevant_a = { a for a,b in y_true if b }
    non_relevant_a = { a for a,b in y_true if not b }

    relevant_a_table={}
    coe =  len(non_relevant_a)
    i = 0
    j=0
    for d in pred:
        if d in relevant_a :
            relevant_a_table[d]=i
        if d in non_relevant_a :
            i+=1


    if len(relevant_a_table) > 0 and len(relevant_a) > 0:

        if i == 0 :
            return 1.0

        dividend = min(i,len(relevant_a))

        sumands = { k: (1 - float(min(len(relevant_a),v) )/dividend) for k,v in relevant_a_table.items()}

        return sum(sumands.values())/len(relevant_a)
    else:
        return 0

def reciprocal_rank(pred,y_true):

    relevant_a = { a for a,b in y_true if b }

    i=1
    for d in pred:
        if d in relevant_a :
            return 1.0/i

        i+=1

    return 0.0

def dcg(pred,y_true):

    relevant_a = { a for a,b in y_true if b }

    s = 0.0

    i=1
    for d in pred:
        if d in relevant_a :
            if i==1:
                s += 1.0
            else:
                s += 1.0 / math.log2(i)

        i+=1

    n = 1.0
    i = 2

    for d in range(2,len(relevant_a)+1):
        n += 1.0 / math.log2(i)
        i+=1

    return s / n

def ap(pred,y_true):

    judged_relevant = set()
    judged_nonrelevant = set()

    for d,bo in y_true:
        judged_relevant.add(d) if bo else judged_nonrelevant.add(d)

    judged_and_retrieved = pred.intersection(judged_relevant).union(pred.intersection(judged_nonrelevant) )

    k = 1
    corrects = 0
    l=[]

    for d in pred:
        if d in judged_and_retrieved:
            if d in judged_relevant:
                corrects+=1
            k+=1

            l.append( float(corrects)/k )

    if k == 1:
        return 0
    else:
        return sum(l)/len(l)
