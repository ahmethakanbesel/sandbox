# Regex vs HTML Parsing for information extract

## Benchmark Results

```
BenchmarkExtractLinkByParsing-8                   881247              1327 ns/op
BenchmarkExtractLinkByParsing-8                   883822              1330 ns/op
BenchmarkExtractLinkByParsing-8                   853792              1332 ns/op
BenchmarkExtractLinkByParsing-8                   888178              1339 ns/op
BenchmarkExtractLinkByParsing-8                   885589              1338 ns/op
BenchmarkExtractLinkByRegexStringSubmatch-8       445767              2563 ns/op
BenchmarkExtractLinkByRegexStringSubmatch-8       455166              2559 ns/op
BenchmarkExtractLinkByRegexStringSubmatch-8       469885              2537 ns/op
BenchmarkExtractLinkByRegexStringSubmatch-8       470676              2538 ns/op
BenchmarkExtractLinkByRegexStringSubmatch-8       463108              2557 ns/op
BenchmarkExtractLinkByRegexFindString-8           395486              2992 ns/op
BenchmarkExtractLinkByRegexFindString-8           391954              3012 ns/op
BenchmarkExtractLinkByRegexFindString-8           391653              3000 ns/op
BenchmarkExtractLinkByRegexFindString-8           392954              3025 ns/op
BenchmarkExtractLinkByRegexFindString-8           391657              2999 ns/op
```
