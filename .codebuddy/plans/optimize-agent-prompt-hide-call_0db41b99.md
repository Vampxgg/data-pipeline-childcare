---
name: optimize-agent-prompt-hide-call
overview: 根据用户反馈，更新 `调度智能体.md`，明确 `<call>` 标签为内部系统指令，严禁输出给用户，确保仅 `<notification>` 对用户可见。
todos:
  - id: update-prompt-file
    content: Update '调度智能体.md' to mark call tag as internal and add anti-leak constraints
    status: completed
---

## User Requirements

- **Hide Tool Calls**: The `<call>` tag must be treated as **Strictly Internal** information, just like `<thought>`. It must not be displayed to the user.
- **Strict Separation**: Ensure that neither internal logic (thoughts) nor system instructions (calls) leak into the user-facing `<notification>` channel.

## Core Features

1.  **Protocol Update**: Redefine `<call>` in the Output Protocol as a "Hidden from User" section.
2.  **Constraint Enforcement**: Add explicit constraints to prevent `<call>` content from appearing in notifications.

## Implementation Details

### File Modification

Target File: `e:/Vampxgg/数据管道pipeline/托育/调度智能体.md`

#### 1. Update Output Protocol

Modify the `<call>` section (Section 3) to explicitly state:

-   **STRICTLY INTERNAL**: Mark the tag as internal.
-   **Hidden from User**: Explicitly forbid displaying this content to the user.

#### 2. Update Constraints

Modify Constraint #6 (`No Thought Leak`) to `No Internal Leak`:

-   Expand the scope to include both `<thought>` and `<call>` tags.
-   Strictly prohibit their content from appearing in `<notification>`.