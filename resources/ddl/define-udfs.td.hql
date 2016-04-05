create temporary function hivemall_version as 'hivemall.HivemallVersionUDF';

-- Binary classification 
create temporary function train_perceptron as 'hivemall.classifier.PerceptronUDTF';
create temporary function train_pa as 'hivemall.classifier.PassiveAggressiveUDTF';
create temporary function train_pa1 as 'hivemall.classifier.PassiveAggressiveUDTF$PA1';
create temporary function train_pa2 as 'hivemall.classifier.PassiveAggressiveUDTF$PA2';
create temporary function train_cw as 'hivemall.classifier.ConfidenceWeightedUDTF';
create temporary function train_arow as 'hivemall.classifier.AROWClassifierUDTF';
create temporary function train_arowh as 'hivemall.classifier.AROWClassifierUDTF$AROWh';
create temporary function train_scw as 'hivemall.classifier.SoftConfideceWeightedUDTF$SCW1';
create temporary function train_scw2 as 'hivemall.classifier.SoftConfideceWeightedUDTF$SCW2';
create temporary function train_adagrad_rda as 'hivemall.classifier.AdaGradRDAUDTF';

--  Multiclass classification
create temporary function train_multiclass_perceptron as 'hivemall.classifier.multiclass.MulticlassPerceptronUDTF';
create temporary function train_multiclass_pa as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF';
create temporary function train_multiclass_pa1 as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1';
create temporary function train_multiclass_pa2 as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2';
create temporary function train_multiclass_cw as 'hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF';
create temporary function train_multiclass_arow as 'hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF';
create temporary function train_multiclass_scw as 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1';
create temporary function train_multiclass_scw2 as 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2';

-- Similarity functions
create temporary function cosine_similarity as 'hivemall.knn.similarity.CosineSimilarityUDF';
create temporary function jaccard_similarity as 'hivemall.knn.similarity.JaccardIndexUDF';
create temporary function angular_similarity as 'hivemall.knn.similarity.AngularSimilarityUDF';
create temporary function euclid_similarity as 'hivemall.knn.similarity.EuclidSimilarity';
create temporary function distance2similarity as 'hivemall.knn.similarity.Distance2SimilarityUDF';

-- Distance functions
create temporary function popcnt as 'hivemall.knn.distance.PopcountUDF';
create temporary function kld as 'hivemall.knn.distance.KLDivergenceUDF';
create temporary function hamming_distance as 'hivemall.knn.distance.HammingDistanceUDF';
create temporary function euclid_distance as 'hivemall.knn.distance.EuclidDistanceUDF';
create temporary function cosine_distance as 'hivemall.knn.distance.CosineDistanceUDF';
create temporary function angular_distance as 'hivemall.knn.distance.AngularDistanceUDF';
create temporary function jaccard_distance as 'hivemall.knn.distance.JaccardDistanceUDF';
create temporary function manhattan_distance as 'hivemall.knn.distance.ManhattanDistanceUDF';
create temporary function minkowski_distance as 'hivemall.knn.distance.MinkowskiDistanceUDF';

-- LSH functions
create temporary function minhashes as 'hivemall.knn.lsh.MinHashesUDF';
create temporary function minhash as 'hivemall.knn.lsh.MinHashUDTF';
create temporary function bbit_minhash as 'hivemall.knn.lsh.bBitMinHashUDF';

-- Voting functions
create temporary function voted_avg as 'hivemall.ensemble.bagging.VotedAvgUDAF';
create temporary function weight_voted_avg as 'hivemall.ensemble.bagging.WeightVotedAvgUDAF';

-- Misc functions
create temporary function max_label as 'hivemall.ensemble.MaxValueLabelUDAF';
create temporary function maxrow as 'hivemall.ensemble.MaxRowUDAF';
create temporary function argmin_kld as 'hivemall.ensemble.ArgminKLDistanceUDAF';

-- Hashing functions
create temporary function mhash as 'hivemall.ftvec.hashing.MurmurHash3UDF';
create temporary function sha1 as 'hivemall.ftvec.hashing.Sha1UDF';
create temporary function array_hash_values as 'hivemall.ftvec.hashing.ArrayHashValuesUDF';
create temporary function prefixed_hash_values as 'hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF';

-- Pairing functions
create temporary function polynomial_features as 'hivemall.ftvec.pairing.PolynomialFeaturesUDF';
create temporary function powered_features as 'hivemall.ftvec.pairing.PoweredFeaturesUDF';

-- Scaling functions
create temporary function rescale as 'hivemall.ftvec.scaling.RescaleUDF';
create temporary function rescale_fv as 'hivemall.ftvec.scaling.RescaleUDF';
create temporary function zscore as 'hivemall.ftvec.scaling.ZScoreUDF';
create temporary function normalize as 'hivemall.ftvec.scaling.L2NormalizationUDF';

-- Feature engineering functions 
create temporary function amplify as 'hivemall.ftvec.amplify.AmplifierUDTF';
create temporary function rand_amplify as 'hivemall.ftvec.amplify.RandomAmplifierUDTF';
create temporary function addBias as 'hivemall.ftvec.AddBiasUDF';
create temporary function add_bias as 'hivemall.ftvec.AddBiasUDF';
create temporary function sort_by_feature as 'hivemall.ftvec.SortByFeatureUDF';
create temporary function extract_feature as 'hivemall.ftvec.ExtractFeatureUDF';
create temporary function extract_weight as 'hivemall.ftvec.ExtractWeightUDF';
create temporary function add_feature_index as 'hivemall.ftvec.AddFeatureIndexUDF';
create temporary function feature as 'hivemall.ftvec.FeatureUDF';
create temporary function feature_index as 'hivemall.ftvec.FeatureIndexUDF';

-- Feature converting functions
create temporary function conv2dense as 'hivemall.ftvec.conv.ConvertToDenseModelUDAF';
create temporary function to_dense_features as 'hivemall.ftvec.conv.ToDenseFeaturesUDF';
create temporary function to_dense as 'hivemall.ftvec.conv.ToDenseFeaturesUDF';
create temporary function to_sparse_features as 'hivemall.ftvec.conv.ToSparseFeaturesUDF';
create temporary function to_sparse as 'hivemall.ftvec.conv.ToSparseFeaturesUDF';
create temporary function quantify as 'hivemall.ftvec.conv.QuantifyColumnsUDTF';

-- Feature transformers
create temporary function vectorize_features as 'hivemall.ftvec.trans.VectorizeFeaturesUDF';
create temporary function categorical_features as 'hivemall.ftvec.trans.CategoricalFeaturesUDF';
create temporary function indexed_features as 'hivemall.ftvec.trans.IndexedFeatures';
create temporary function quantified_features as 'hivemall.ftvec.trans.QuantifiedFeaturesUDTF';
create temporary function quantitative_features as 'hivemall.ftvec.trans.QuantitativeFeaturesUDF';
create temporary function binarize_label as 'hivemall.ftvec.trans.BinarizeLabelUDTF';

-- sampling of feature vectors
-- create temporary function bpr_sampling as 'hivemall.ftvec.sampling.BprSamplingUDTF';

-- ftvec/text functions
create temporary function tf as 'hivemall.ftvec.text.TermFrequencyUDAF';

-- Regression functions
create temporary function logress as 'hivemall.regression.LogressUDTF';
create temporary function train_logistic_regr as 'hivemall.regression.LogressUDTF';
create temporary function train_pa1_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF';
create temporary function train_pa1a_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a';
create temporary function train_pa2_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2';
create temporary function train_pa2a_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a';
create temporary function train_arow_regr as 'hivemall.regression.AROWRegressionUDTF';
create temporary function train_arowe_regr as 'hivemall.regression.AROWRegressionUDTF$AROWe';
create temporary function train_arowe2_regr as 'hivemall.regression.AROWRegressionUDTF$AROWe2';
create temporary function train_adadelta_regr as 'hivemall.regression.AdaDeltaUDTF';
create temporary function pa1_regress as 'hivemall.regression.PassiveAggressiveRegressionUDTF';
create temporary function pa1a_regress as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a';
create temporary function pa2_regress as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2';
create temporary function pa2a_regress as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a';
create temporary function arow_regress as 'hivemall.regression.AROWRegressionUDTF';
create temporary function arowe_regress as 'hivemall.regression.AROWRegressionUDTF$AROWe';
create temporary function arowe2_regress as 'hivemall.regression.AROWRegressionUDTF$AROWe2';

-- Array functions
create temporary function float_array as 'hivemall.tools.array.AllocFloatArrayUDF';
create temporary function array_remove as 'hivemall.tools.array.ArrayRemoveUDF';
create temporary function sort_and_uniq_array as 'hivemall.tools.array.SortAndUniqArrayUDF';
create temporary function subarray_endwith as 'hivemall.tools.array.SubarrayEndWithUDF';
create temporary function subarray_startwith as 'hivemall.tools.array.SubarrayStartWithUDF';
create temporary function collect_all as 'hivemall.tools.array.CollectAllUDAF';
create temporary function concat_array as 'hivemall.tools.array.ConcatArrayUDF';
create temporary function subarray as 'hivemall.tools.array.SubarrayUDF';
create temporary function array_avg as 'hivemall.tools.array.ArrayAvgGenericUDAF';
create temporary function array_sum as 'hivemall.tools.array.ArraySumUDAF';
create temporary function to_string_array as 'hivemall.tools.array.ToStringArrayUDF';

-- Compression functions
create temporary function inflate as 'hivemall.tools.compress.InflateUDF';
create temporary function deflate as 'hivemall.tools.compress.DeflateUDF';

-- Map functions
create temporary function map_get_sum as 'hivemall.tools.map.MapGetSumUDF';
create temporary function map_tail_n as 'hivemall.tools.map.MapTailNUDF';
create temporary function to_map as 'hivemall.tools.map.UDAFToMap';
create temporary function to_ordered_map as 'hivemall.tools.map.UDAFToOrderedMap';

-- Math functions
create temporary function sigmoid as 'hivemall.tools.math.SigmoidGenericUDF';

-- mapred functions
create temporary function taskid as 'hivemall.tools.mapred.TaskIdUDF';
create temporary function jobid as 'hivemall.tools.mapred.JobIdUDF';
create temporary function rowid as 'hivemall.tools.mapred.RowIdUDF';

-- misc functions
create temporary function generate_series as 'hivemall.tools.GenerateSeriesUDTF';
create temporary function convert_label as 'hivemall.tools.ConvertLabelUDF';
create temporary function x_rank as 'hivemall.tools.RankSequenceUDF';
create temporary function each_top_k as 'hivemall.tools.EachTopKUDTF';

-- Text processing functions 
create temporary function tokenize as 'hivemall.tools.text.TokenizeUDF';
create temporary function is_stopword as 'hivemall.tools.text.StopwordUDF';
create temporary function split_words as 'hivemall.tools.text.SplitWordsUDF';
create temporary function normalize_unicode as 'hivemall.tools.text.NormalizeUnicodeUDF';
create temporary function base91 as 'hivemall.tools.text.Base91UDF';
create temporary function unbase91 as 'hivemall.tools.text.Unbase91UDF';

-- Dataset generator functions
create temporary function lr_datagen as 'hivemall.dataset.LogisticRegressionDataGeneratorUDTF';

-- Evaluating functions
create temporary function f1score as 'hivemall.evaluation.FMeasureUDAF';
create temporary function mae as 'hivemall.evaluation.MeanAbsoluteErrorUDAF';
create temporary function mse as 'hivemall.evaluation.MeanSquaredErrorUDAF';
create temporary function rmse as 'hivemall.evaluation.RootMeanSquaredErrorUDAF';
create temporary function r2 as 'hivemall.evaluation.R2UDAF';

-- Matrix Factorization 
create temporary function mf_predict as 'hivemall.mf.MFPredictionUDF';
create temporary function train_mf_sgd as 'hivemall.mf.MatrixFactorizationSGDUDTF';
create temporary function train_mf_adagrad as 'hivemall.mf.MatrixFactorizationAdaGradUDTF';
-- create temporary function train_bprmf as 'hivemall.mf.BPRMatrixFactorizationUDTF';
-- create temporary function bprmf_predict as 'hivemall.mf.BPRMFPredictionUDF';

-- Factorization Machine
create temporary function fm_predict as 'hivemall.fm.FMPredictGenericUDAF';
create temporary function train_fm as 'hivemall.fm.FactorizationMachineUDTF';

-- Smile related features
create temporary function train_randomforest_classifier as 'hivemall.smile.classification.RandomForestClassifierUDTF';
create temporary function train_randomforest_regressor as 'hivemall.smile.regression.RandomForestRegressionUDTF';
create temporary function train_randomforest_regr as 'hivemall.smile.regression.RandomForestRegressionUDTF';
create temporary function tree_predict as 'hivemall.smile.tools.TreePredictUDF';
create temporary function rf_ensemble as 'hivemall.smile.tools.RandomForestEnsembleUDAF';
create temporary function guess_attribute_types as 'hivemall.smile.tools.GuessAttributesUDF';

-- NLP features
create temporary function tokenize_ja as 'hivemall.nlp.tokenizer.KuromojiUDF';


