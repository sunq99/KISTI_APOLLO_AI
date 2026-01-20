
import os
import pickle

from app.model2.exec_file import paths
from app.model2.exec_file.parse import parse_args


args = parse_args()

if not args.run_scratch:
    print('Model loading ...')
    rec_sys = pickle.load(open(paths.RECSYS_PATH, 'rb'))
    print('Model loading completed!!')
    rec_sys.src_th = args.src_threshold
    rec_sys.pool_th = args.pool_threshold

    rec_sys.ds = []
    rec_sys.pool_emb = {}
    rec_sys.pool_logits = {}
    rec_sys.pool_pred = {}

    pickle.dump(rec_sys, open(paths.DATA_ROOT+'/reduce.pkl', 'wb'))
    print('Model mem reduction completed!!')
