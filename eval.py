
def eval(qtrain, pred, beta=0.5):
    tkeys = list(qtrain.keys())
    results = []
    for topic in tkeys:

        judged_relevant = set()
        judged_nonrelevant = set()

        for d,bo in qtrain[topic]:
            judged_relevant.add(d) if bo else judged_nonrelevant.add(d)

        answers = pred[topic]

        precision, recall, fbeta = classic_metrics(answers, judged_relevant, judged_nonrelevant, beta=beta)

        entry = {}
        entry["precision"] = precision
        entry["recall"] = recall
        entry["fbeta"] = fbeta
        results.append( entry )

    ps = [ i["precision"] for i in results]
    rs = [ i["recall"] for i in results ]
    rb = [ entry["fbeta"] for i in results ]

    reduce_ps = sum(ps)/len(ps)
    reduce_rs = sum(rs)/len(rs)
    reduce_rb = sum(rb)/len(rb)

    return results, reduce_ps, reduce_rs, reduce_rb

def classic_metrics( answers, judged_relevant, judged_nonrelevant, beta=0.5):
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
    for d,_ in pred:
        if d in relevant_a :
            relevant_a_table[d]=i
        if d in non_relevant_a :
            i+=1

    if len(relevant_a_table) > 0 :

        dividend = min(i,len(relevant_a))
        #print(dividend)

        sumands = { k: (1 - float(v)/dividend) for k,v in relevant_a_table.items()}
        #print(sumands)
        #print(sum(sumands.values()))
        #print(len(relevant_a))
        return sum(sumands.values())/len(relevant_a)
    else:
        return 0
