## 平行處理資料載入/寫入速度


順便看一下最終消耗的記憶體用量
### 實驗0
#### 實驗0-1(單純讀取)：

| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|------|------------|
| 1,000 | 0.132 | 12 |
| 5,000 | 0.137 | 14 |
| 10,000 | 0.135 | 18 |
| 50,000 | 0.706 | 28 |
| 100,000 | 0.788 | 42 |
| 150,000 | 2.435 | 56 |
| 200,000 | 3.432 | 67 |
| 500,000 | 5.017 | 144 |
| 1,000,000 | 11.232 | 272 |
| 5,000,000 | 46.747 | 1100 |
| 10,000,000 | 130.46 | 646 |

#### 實驗0-2(讀取+寫入)：
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 1,000 | 0.058 | 12 |
| 5,000 | 0.25 | 18 |
| 10,000 | 0.666 | 20 |
| 50,000 | 1.23 | 29 |
| 100,000 | 2.426 | 44 |
| 150,000 | 2.719 | 59 |
| 200,000 | 3.855 | 70 |
| 500,000 | 7.27 | 146 |
| 1,000,000 | 12.502 | 274 |
| 5,000,000 | 76.849 | 1300 |
| 10,000,000 | 251.022 | 760 |

### 實驗1 整份讀取+批次寫入
* 每次批次都要清除記憶體
#### 1-1 每10000筆寫入一次
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 | 0.307 | 20 |
| 50,000 | 4.281 | 30 |
| 100,000 | 3.074 | 44 |
| 150,000 | 3.99 | 57 |
| 200,000 | 5.296 | 70 |
| 500,000 | 15.862 | 146 |
| 1,000,000 | 41.27 | 276 |
| 5,000,000 | 649.278 | 1300 |
| 10,000,000 | - | - |
#### 1-2 每50000
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 50,000 | 4.281 | 30 |
| 100,000 | 3.083 | 44 |
| 150,000 | 3.85 | 57 |
| 200,000 | 4.228 | 70 |
| 500,000 | 10.328 | 91 |
| 1,000,000 | 19.598 | 275 |
| 5,000,000 | 219.781| 1300 |
| 10,000,000 | - | - |
#### 1-2 每100000
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 150,000 | 3.02 | 57 |
| 200,000 | 4.262 | 71 |
| 500,000 | 9.164 | 146 |
| 1,000,000 | 16.53 | 274 |
| 5,000,000 | 133.613 | 809 |
| 10,000,000 | - | - |

### 實驗2 分批讀取 + 批次寫入
#### 2-1 每10000筆一次
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 | 0.429 | 20 |
| 50,000 | 3.335 | 22 |
| 100,000 | 6.042 | 66 ->22 |
| 150,000 | 11.293 | 94 -> 22 |
| 200,000 | 20.228 | 56 |
| 500,000 | 100.443 | 67 |
| 1,000,000 | 385.162 | 530 |
| 5,000,000 | - | - |
| 10,000,000 | - | - |


#### 每50000筆一次
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 |- | - |
| 50,000 | 0.918 | 21 |
| 100,000 | 2.428 | 20 |
| 150,000 | 3.887 | 21 |
| 200,000 | 4.099 | 21 |
| 500,000 | 9.954 | 21 |
| 1,000,000 | 38.197 | 21 |
| 5,000,000 | 520.18 | 21 |
| 10,000,000 | - | - |
#### 每50000筆一次 (GC)
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 |- | - |
| 50,000 | 1.142 | 30 |
| 100,000 | 1.497 | 41 |
| 150,000 | 2.992 | 46 |
| 200,000 | 4.686 | 51 |
| 500,000 | 7.435 | 63 |
| 1,000,000 | 12.957 | 66 |
| 5,000,000 | 158.046 | 65 |
| 10,000,000 | - | - |

#### 每50000筆一次 (固態硬碟＋GC)
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 |- | - |
| 50,000 | 1.235 | 30 |
| 100,000 | 2.294 | 41 |
| 150,000 | 2.968 | 47 |
| 200,000 | 3.632 | 65 |
| 500,000 | 6.999 | 65 |
| 1,000,000 | 12.79 | 65 |
| 5,000,000 | 158.046 | 65 |
| 10,000,000 | - | - |

#### 讀取 + 寫入 五條執行緒
| 筆數 | 秒數(s) | 記憶體用量(mb) |
|------|-----|------------|
| 10,000 | 0.099 | 15 |
| 50,000 | 0.248 | 22 |
| 100,000 | 0.389 | 15 |
| 150,000 | 0.49 | 15 |
| 200,000 | 0.506| 15 |
| 500,000 | 1.309 | 15 |
| 1,000,000 | 2.695 | 15 |
| 5,000,000 | 55 | 15 |
| 10,000,000 | 115.948 | 164 |