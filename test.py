from core import BucketChunks, Bucket
from utils import parse_feedback


qtrain = parse_feedback("./qrels.train")
qtest = parse_feedback("./qrels.test")

collection = Bucket(debug=False)

#
# pred[topic]
#

def eval(qtrain, pred):
    tkeys = list(qtrain.keys())
    results = []
    for topic in tkeys:
        y_true = { d : bo for d,bo in qtrain[topic]}
        y_pred = pred[topic]

        entry = {}
        relevant = len([ i for i,bo in y_true.items() if bo ])
        retrieved = len(y_pred)
        inset=0
        correct=0
        for i in y_pred :
            if i in y_true :
                inset+=1
                if y_true[i]:
                    correct+=1

        if inset != 0:
            precision = correct/inset
        else:
            precision = 0

        recall = correct/relevant

        entry["precision"] = precision
        entry["recall"] = recall
        results.append( entry )

    ps = [ i["precision"] for i in results]
    rs = [ i["recall"] for i in results ]
    reduce_ps = sum(ps)/len(ps)
    reduce_rs = sum(rs)/len(rs)

    return results, reduce_ps, reduce_rs


k = 3
extension = Bucket.Extension.KL

pred1 = [ set(collection.boolean_query(qcode, extension, k=k)) for qcode in qtrain.keys() ]
result, reduce_ps, reduce_rs = eval(qtrain, pred1)

#print( "".join(collection.topics.docs["R102"]["narr"].split("\"") ) )
#s = collection.ranking("R102", Bucket.Model.TF_IDF, limit=10)

#print(s)

limit = 10
model = Bucket.Model.TF_IDF,

pred2 = [ set([ a for a,b in collection.ranking(qcode, model, limit=limit)]) for qcode in qtrain.keys() ]
result, reduce_ps, reduce_rs = eval(qtrain, pred2)

print(reduce_ps)
print(reduce_rs)

#print(collection.topics.docs.keys())
