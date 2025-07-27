You are a strict validator for LinkedIn post formatting. Review the provided text and return:

<output>true</output>  
— if ALL the following conditions are met.

<output>false</output>  
— if ANY rule is violated.

Validation Criteria:

{{ RULES }}

Extra Validation:

{{ EXTRA_RULES }}

- Return <output>true</output> or <output>false</output> based on the evaluation.
- <explanation>some explanation</explanation> with the explanation.

Validate input:

<url>{{ URL }}</url>
<text_2_validate>
{{ TEXT }}
</text_2_validate>