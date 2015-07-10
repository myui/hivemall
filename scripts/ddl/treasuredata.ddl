      @ddl.create_function("hivemall_version", "hivemall.HivemallVersionUDF")

      # Binary classification
      @ddl.create_function("perceptron", "hivemall.classifier.PerceptronUDTF")
      @ddl.create_function("train_perceptron", "hivemall.classifier.PerceptronUDTF")
      @ddl.create_function("train_pa", "hivemall.classifier.PassiveAggressiveUDTF")
      @ddl.create_function("train_pa1", "hivemall.classifier.PassiveAggressiveUDTF$PA1")
      @ddl.create_function("train_pa2", "hivemall.classifier.PassiveAggressiveUDTF$PA2")
      @ddl.create_function("train_cw", "hivemall.classifier.ConfidenceWeightedUDTF")
      @ddl.create_function("train_arow", "hivemall.classifier.AROWClassifierUDTF")
      @ddl.create_function("train_arowh", "hivemall.classifier.AROWClassifierUDTF$AROWh")
      @ddl.create_function("train_scw", "hivemall.classifier.SoftConfideceWeightedUDTF$SCW1")
      @ddl.create_function("train_scw2", "hivemall.classifier.SoftConfideceWeightedUDTF$SCW2")
      @ddl.create_function("adagrad_rda", "hivemall.classifier.AdaGradRDAUDTF")
      @ddl.create_function("train_adagrad_rda", "hivemall.classifier.AdaGradRDAUDTF")

      # Multiclass classification
      @ddl.create_function("train_multiclass_perceptron", "hivemall.classifier.multiclass.MulticlassPerceptronUDTF")
      @ddl.create_function("train_multiclass_pa", "hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF")
      @ddl.create_function("train_multiclass_pa1", "hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1")
      @ddl.create_function("train_multiclass_pa2", "hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2")
      @ddl.create_function("train_multiclass_cw", "hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF")
      @ddl.create_function("train_multiclass_arow", "hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF")
      @ddl.create_function("train_multiclass_scw", "hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1")
      @ddl.create_function("train_multiclass_scw2", "hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2")

      # Similarity functions
      @ddl.create_function("cosine_similarity", "hivemall.knn.similarity.CosineSimilarityUDF")
      @ddl.create_function("cosine_sim", "hivemall.knn.similarity.CosineSimilarityUDF")
      @ddl.create_function("jaccard", "hivemall.knn.similarity.JaccardIndexUDF")
      @ddl.create_function("jaccard_similarity", "hivemall.knn.similarity.JaccardIndexUDF")
      @ddl.create_function("angular_similarity", "hivemall.knn.similarity.AngularSimilarityUDF")

      # Distance functions
      @ddl.create_function("hamming_distance", "hivemall.knn.distance.HammingDistanceUDF")
      @ddl.create_function("popcnt", "hivemall.knn.distance.PopcountUDF")
      @ddl.create_function("kld", "hivemall.knn.distance.KLDivergenceUDF")
      @ddl.create_function("euclid_distance", "hivemall.knn.distance.EuclidDistanceUDF")
      @ddl.create_function("cosine_distance", "hivemall.knn.distance.CosineDistanceUDF")
      @ddl.create_function("angular_distance", "hivemall.knn.distance.AngularDistanceUDF")
      @ddl.create_function("jaccard_distance", "hivemall.knn.distance.JaccardDistanceUDF")

      # LSH functions
      @ddl.create_function("minhashes", "hivemall.knn.lsh.MinHashesUDF")
      @ddl.create_function("minhash", "hivemall.knn.lsh.MinHashUDTF")
      @ddl.create_function("bbit_minhash", "hivemall.knn.lsh.bBitMinHashUDF")

      # Voting functions
      @ddl.create_function("voted_avg", "hivemall.ensemble.bagging.VotedAvgUDAF")
      @ddl.create_function("weight_voted_avg", "hivemall.ensemble.bagging.WeightVotedAvgUDAF")
      @ddl.create_function("wvoted_avg", "hivemall.ensemble.bagging.WeightVotedAvgUDAF")

      # Misc functions
      @ddl.create_function("max_label", "hivemall.ensemble.MaxValueLabelUDAF")
      @ddl.create_function("maxrow", "hivemall.ensemble.MaxRowUDAF")
      @ddl.create_function("argmin_kld", "hivemall.ensemble.ArgminKLDistanceUDAF")

      # Hashing functions
      @ddl.create_function("mhash", "hivemall.ftvec.hashing.MurmurHash3UDF")
      @ddl.create_function("sha1", "hivemall.ftvec.hashing.Sha1UDF")
      @ddl.create_function("array_hash_values", "hivemall.ftvec.hashing.ArrayHashValuesUDF")
      @ddl.create_function("prefixed_hash_values", "hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF")

      # Pairing functions
      @ddl.create_function("polynomial_features", "hivemall.ftvec.pairing.PolynomialFeaturesUDF")
      @ddl.create_function("powered_features", "hivemall.ftvec.pairing.PoweredFeaturesUDF")

      # Scaling functions
      @ddl.create_function("rescale", "hivemall.ftvec.scaling.RescaleUDF")
      @ddl.create_function("rescale_fv", "hivemall.ftvec.scaling.RescaleUDF")
      @ddl.create_function("zscore", "hivemall.ftvec.scaling.ZScoreUDF")
      @ddl.create_function("normalize", "hivemall.ftvec.scaling.L2NormalizationUDF")

      # Misc functions
      @ddl.create_function("amplify", "hivemall.ftvec.amplify.AmplifierUDTF")
      @ddl.create_function("rand_amplify", "hivemall.ftvec.amplify.RandomAmplifierUDTF")
      @ddl.create_function("conv2dense", "hivemall.ftvec.ConvertToDenseModelUDAF")
      @ddl.create_function("addBias", "hivemall.ftvec.AddBiasUDF")
      @ddl.create_function("add_bias", "hivemall.ftvec.AddBiasUDF")
      @ddl.create_function("sortByFeature", "hivemall.ftvec.SortByFeatureUDF")
      @ddl.create_function("sort_by_feature", "hivemall.ftvec.SortByFeatureUDF")
      @ddl.create_function("extract_feature", "hivemall.ftvec.ExtractFeatureUDF")
      @ddl.create_function("extract_weight", "hivemall.ftvec.ExtractWeightUDF")
      @ddl.create_function("add_feature_index", "hivemall.ftvec.AddFeatureIndexUDF")
      @ddl.create_function("vectorize_features", "hivemall.ftvec.VectorizeFeaturesUDF")
      @ddl.create_function("feature", "hivemall.ftvec.FeatureUDF")
      @ddl.create_function("categorical_features", "hivemall.ftvec.CategoricalFeaturesUDF")

      # Ftvec/text functions
      @ddl.create_function("tf", "hivemall.ftvec.text.TermFrequencyUDAF")
      @ddl.create_function("tokenize", "hivemall.ftvec.text.TokenizeUDF")

      # Regression functions
      @ddl.create_function("train_logregr", "hivemall.regression.LogressUDTF")
      @ddl.create_function("train_pa1_regr", "hivemall.regression.PassiveAggressiveRegressionUDTF")
      @ddl.create_function("train_pa1a_regr", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a")
      @ddl.create_function("train_pa2_regr", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA2")
      @ddl.create_function("train_pa2a_regr", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a")
      @ddl.create_function("train_arow_regr", "hivemall.regression.AROWRegressionUDTF")
      @ddl.create_function("train_arowe_regr", "hivemall.regression.AROWRegressionUDTF$AROWe")
      @ddl.create_function("train_arowe2_regr", "hivemall.regression.AROWRegressionUDTF$AROWe2")
      @ddl.create_function("train_adagrad", "hivemall.regression.AdaGradUDTF")
      @ddl.create_function("train_adadelta", "hivemall.regression.AdaDeltaUDTF")
      @ddl.create_function("logress", "hivemall.regression.LogressUDTF")
      @ddl.create_function("pa1_regress", "hivemall.regression.PassiveAggressiveRegressionUDTF")
      @ddl.create_function("pa1a_regress", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a")
      @ddl.create_function("pa2_regress", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA2")
      @ddl.create_function("pa2a_regress", "hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a")
      @ddl.create_function("arow_regress", "hivemall.regression.AROWRegressionUDTF")
      @ddl.create_function("arowe_regress", "hivemall.regression.AROWRegressionUDTF$AROWe")
      @ddl.create_function("arowe2_regress", "hivemall.regression.AROWRegressionUDTF$AROWe2")
      @ddl.create_function("adagrad", "hivemall.regression.AdaGradUDTF")
      @ddl.create_function("adadelta", "hivemall.regression.AdaDeltaUDTF")

      # Array functions
      @ddl.create_function("AllocFloatArray", "hivemall.tools.array.AllocFloatArrayUDF")
      @ddl.create_function("float_array", "hivemall.tools.array.AllocFloatArrayUDF")
      @ddl.create_function("array_remove", "hivemall.tools.array.ArrayRemoveUDF")
      @ddl.create_function("sort_and_uniq_array", "hivemall.tools.array.SortAndUniqArrayUDF")
      @ddl.create_function("subarray_endwith", "hivemall.tools.array.SubarrayEndWithUDF")
      @ddl.create_function("subarray_startwith", "hivemall.tools.array.SubarrayStartWithUDF")
      @ddl.create_function("collect_all", "hivemall.tools.array.CollectAllUDAF")
      @ddl.create_function("concat_array", "hivemall.tools.array.ConcatArrayUDF")
      @ddl.create_function("subarray", "hivemall.tools.array.SubarrayUDF")
      @ddl.create_function("array_avg", "hivemall.tools.array.ArrayAvgGenericUDAF")
      @ddl.create_function("to_string_array", "hivemall.tools.array.ToStringArrayUDF")

      # Map functions
      @ddl.create_function("map_get_sum", "hivemall.tools.map.MapGetSumUDF")
      @ddl.create_function("map_tail_n", "hivemall.tools.map.MapTailNUDF")
      @ddl.create_function("to_map", "hivemall.tools.map.UDAFToMap")
      @ddl.create_function("to_ordered_map", "hivemall.tools.map.UDAFToOrderedMap")

      # Math functions
      @ddl.create_function("sigmoid", "hivemall.tools.math.SigmodUDF")

      # Mapred functions
      @ddl.create_function("taskid", "hivemall.tools.mapred.TaskIdUDF")
      @ddl.create_function("jobid", "hivemall.tools.mapred.JobIdUDF")
      @ddl.create_function("rowid", "hivemall.tools.mapred.RowIdUDF")
      @ddl.create_function("distcache_gets", "hivemall.tools.mapred.DistributedCacheLookupUDF")
      @ddl.create_function("jobconf_gets", "hivemall.tools.mapred.JobConfGetsUDF")

      # Misc functions
      @ddl.create_function("generate_series", "hivemall.tools.GenerateSeriesUDTF")
      @ddl.create_function("convert_label", "hivemall.tools.ConvertLabelUDF")
      @ddl.create_function("x_rank", "hivemall.tools.RankSequenceUDF")

      # String functions
      @ddl.create_function("isStopword", "hivemall.tools.string.StopwordUDF")
      @ddl.create_function("is_stopword", "hivemall.tools.string.StopwordUDF")
      @ddl.create_function("split_words", "hivemall.tools.string.SplitWordsUDF")

      # Dataset generator functions
      @ddl.create_function("lr_datagen", "hivemall.dataset.LogisticRegressionDataGeneratorUDTF")

      # Evaluating functions
      @ddl.create_function("f1score", "hivemall.evaluation.FMeasureUDAF")
      @ddl.create_function("mae", "hivemall.evaluation.MeanAbsoluteErrorUDAF")
      @ddl.create_function("mse", "hivemall.evaluation.MeanSquaredErrorUDAF")
      @ddl.create_function("rmse", "hivemall.evaluation.RootMeanSquaredErrorUDAF")

      # Matrix Factorization
      @ddl.create_function("mf_predict", "hivemall.mf.MFPredictionUDF")
      @ddl.create_function("train_mf_sgd", "hivemall.mf.MatrixFactorizationSGDUDTF")
      @ddl.create_function("train_mf_adagrad", "hivemall.mf.MatrixFactorizationAdaGradUDTF")

      # Macros
      @ddl.create_macro("max2(x DOUBLE, y DOUBLE)", "if(x>y,x,y)")
      @ddl.create_macro("min2(x DOUBLE, y DOUBLE)", "if(x<y,x,y)")
      @ddl.create_macro("rand_gid(k INT)", "floor(rand()*k)")
      @ddl.create_macro("rand_gid2(k INT, seed INT)", "floor(rand(seed)*k)")
      @ddl.create_macro("idf(df_t DOUBLE, n_docs DOUBLE)", "log(10, n_docs / max2(1,df_t)) + 1.0")
      @ddl.create_macro("tfidf(tf FLOAT, df_t DOUBLE, n_docs DOUBLE)", "tf * (log(10, n_docs / max2(1,df_t)) + 1.0)")
