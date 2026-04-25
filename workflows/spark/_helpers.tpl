{{- define "getFiles" -}}
{{- $files := dict -}}
{{- $names := list -}}
{{- range $entry := readDirEntries "./scripts" -}}
{{- if not $entry.IsDir -}}
{{- $names = append $names $entry.Name -}}
{{- end -}}
{{- end -}}
{{- range $name := $names | sortAlpha -}}
{{- $files = set $files $name (readFile (printf "./scripts/%s" $name)) -}}
{{- end -}}
{{- $files | toYaml -}}
{{- end -}}