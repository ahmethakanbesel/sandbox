# titck scraper

Download and extract medicine information leaflets from
https://www.titck.gov.tr/kubkt

## Usage

### Download

```bash
go run cmd/download/main.go --cookie <cookie>
```

### Extract

```bash
go run cmd/extract/main.go
```

## Dependencies

- [pdftottext](https://www.xpdfreader.com/)
