# Tokenizer for English Texts

Hivemall provides simple English text tokenizer UDF that has following syntax:
```sql
tokenize(text input, optional boolean toLowerCase = false)
```

# Tokenizer for Japanese Texts

Hivemall-NLP module provides a Japanese text tokenizer UDF using [Kuromoji](https://github.com/atilika/kuromoji). 

First of all, you need to issue the following DDLs to use the NLP module. Note NLP module is not included in [hivemall-with-dependencies.jar](https://github.com/myui/hivemall/releases).

> add jar /tmp/[hivemall-nlp-xxx-with-dependencies.jar](https://github.com/myui/hivemall/releases);

> source /tmp/[define-additional.hive](https://github.com/myui/hivemall/releases);

The signature of the UDF is as follows:
```sql
tokenize_ja(text input, optional const text mode = "normal", optional const array<string> stopWords, optional const array<string> stopTags)
```
_Caution: `tokenize_ja` is supported since Hivemall v0.4.1 and later._

It's basic usage is as follows:
```sql
select tokenize_ja("kuromojiを使った分かち書きのテストです。第二引数にはnormal/search/extendedを指定できます。デフォルトではnormalモードです。");
```
> ["kuromoji","使う","分かち書き","テスト","第","二","引数","normal","search","extended","指定","デフォルト","normal","モード"]

For detailed APIs, please refer Javadoc of [JapaneseAnalyzer](https://lucene.apache.org/core/5_3_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseAnalyzer.html) as well.