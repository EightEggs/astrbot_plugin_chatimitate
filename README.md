
<div align="center">

![:name](https://count.getloli.com/@:astrbot_plugin_chatimitate?theme=minecraft&padding=6&offset=0&align=top&scale=1&pixelated=1&darkmode=auto)

# astrbot_plugin_chatimitate

_✨ 在 LLM 时代，回归模仿的初心 ✨_  

[![License](https://img.shields.io/badge/License-AGPLv3-green.svg)](https://www.gnu.org/licenses/agpl-3.0.html)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-orange.svg)](https://www.python.org/)
[![AstrBot](https://img.shields.io/badge/AstrBot-4.12%2B-orange.svg)](https://github.com/AstrBotDevs/AstrBot)
[![GitHub](https://img.shields.io/badge/作者-EightEggs-blue.svg)](https://github.com/EightEggs)
[![GitHub](https://img.shields.io/badge/作者-Colin-blue.svg)](https://github.com/cocolinfff)

</div>

## 介绍

本插件受 [Pallas-Bot](<https://github.com/PallasBot/Pallas-Bot>) 项目启发 (部分代码也来自该项目)，旨在让 bot 从群聊记录中**基于关键词地**学习和模仿人类聊天，而不是使用 LLM 生成内容

### 原理

该插件会将群友们的发言都记录在数据库中，根据群友的规律性发言进行回复。

每当群友有一条新发言时，插件会将本条发言记录为上一条发言的可选回复之一，然后在数据库中查找符合条件的本发言的历史回复，从中选择一条进行回复。

以下为一个简单的例子:

```
群友1:诶嘿
群友2:诶嘿是什么意思啊
群友1:诶嘿
群友2:诶嘿是什么意思啊
群友1:诶嘿
群友2:诶嘿是什么意思啊
```

每次有人说诶嘿时，就有人说诶嘿是什么意思啊，这组对话就可以看作规律性发言(表情包同理)。

诶嘿是什么意思啊会被学习为诶嘿的回复3次，而诶嘿会被学习为诶嘿是什么意思啊的回复2次。

在默认配置中，某个回复需要学习次数达到3次后才会将其列为可选答案之一。

因此以后当有群友说诶嘿时，插件就会从数据中查找所有学习次数大于等于3的回复，发现目前有诶嘿是什么意思啊一种，就会有很大概率回复诶嘿是什么意思啊。

## 安装

1. 在 `Astrbot` 的插件市场搜索 `astrbot_plugin_chatimitate`，点击安装

2. 重启 `Astrbot`

## 插件配置

进入插件配置面板进行配置

## 注意事项

- 本插件目前仅在 [NapCat](https://github.com/NapNeko/NapCatQQ) 协议端以及 `Astrbot>=4.12` 测试通过，其他协议端和版本可能会存在一些不兼容问题（以具体情况为准）
- 看到本插件的效果可能需要等待一段时间(数周甚至一个月)，取决于群聊的活跃程度
- 本插件的输出完全依赖于学习到的聊天记录，因此对于插件输出的内容，管理员有责任进行审核和把控
