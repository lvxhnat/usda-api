# Quick Start

```
# In the root directory
pip install -e .
```

## Example Usage
```python
>>> USDA_FAS_API_KEY="{YOUR API KEY HERE}"
>>> res = get_esr_regions(USDA_FAS_API_KEY)
>>> print(res)
```
```
[{'region_id': 1, 'region_name': 'EUROPEAN UNION - 27'}, {'region_id': 2, 'region_name': 'OTHER EUROPE'}, {'region_id': 3, 'region_name': 'EASTERN EUROPE'}, {'region_id': 4, 'region_name': 'FORMER SOVIET UNION-12'}, {'region_id': 5, 'region_name': 'JAPAN'}, {'region_id': 6, 'region_name': 'TAIWAN'}, {'region_id': 7, 'region_name': 'CHINA'}, {'region_id': 8, 'region_name': 'INDIA'}, {'region_id': 9, 'region_name': 'OTHER ASIA AND OCEANIA'}, {'region_id': 10, 'region_name': 'AFRICA'}, {'region_id': 11, 'region_name': 'WESTERN HEMISPHERE'}, {'region_id': 99, 'region_name': 'UNKNOWN'}]
```