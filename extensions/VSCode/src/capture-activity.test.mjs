import assert from "node:assert/strict";
import test from "node:test";

import {
    createCaptureActivityState,
    stopCaptureActivity,
    transitionCaptureActivity,
} from "../.test-output/capture-activity.test.mjs";

function episodeFactory() {
    let next = 1;
    return () => `episode-${next++}`;
}

function observe(state, input, createEpisodeId) {
    return transitionCaptureActivity(
        state,
        {
            nowMs: input.nowMs,
            kind: input.kind,
            filePath: input.filePath,
            fileKey: input.filePath?.toLowerCase(),
            editorGroupId: input.editorGroupId,
            focusChanged: input.focusChanged ?? true,
            focusReason: input.focusReason ?? "active_editor_changed",
        },
        createEpisodeId,
    );
}

test("code-to-code file changes emit focus without a pane switch", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "code", filePath: "A.rs", editorGroupId: 1 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["file_focus_changed"],
    );

    transition = observe(
        transition.state,
        { nowMs: 10, kind: "code", filePath: "B.rs", editorGroupId: 2 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["file_focus_changed"],
    );
    assert.equal(transition.effects[0].from, "code");
    assert.equal(transition.effects[0].to, "code");
    assert.equal(transition.effects[0].fileChanged, true);
});

test("documentation moving between files creates linked file-correct segments", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 100, kind: "doc", filePath: "A.md", editorGroupId: 1 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["file_focus_changed", "doc_session_started"],
    );

    transition = observe(
        transition.state,
        { nowMs: 350, kind: "doc", filePath: "B.md", editorGroupId: 2 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["doc_session_ended", "file_focus_changed", "doc_session_started"],
    );
    assert.deepEqual(transition.effects[0], {
        type: "doc_session_ended",
        filePath: "A.md",
        episodeId: "episode-1",
        segmentIndex: 0,
        durationMs: 250,
        closedBy: "file_focus_changed",
    });
    assert.equal(transition.effects[2].filePath, "B.md");
    assert.equal(transition.effects[2].episodeId, "episode-1");
    assert.equal(transition.effects[2].segmentIndex, 1);
});

test("leaving documentation closes the source file before switching to code", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "doc", filePath: "notes.md", editorGroupId: 1 },
        createEpisodeId,
    );
    transition = observe(
        transition.state,
        { nowMs: 500, kind: "code", filePath: "main.rs", editorGroupId: 1 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["doc_session_ended", "file_focus_changed", "switch_pane"],
    );
    assert.equal(transition.effects[0].filePath, "notes.md");
    assert.equal(transition.effects[0].durationMs, 500);
    assert.equal(transition.effects[2].filePath, "main.rs");
});

test("focus loss closes documentation against its actual file", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "doc", filePath: "notes.md", editorGroupId: 1 },
        createEpisodeId,
    );
    transition = observe(
        transition.state,
        {
            nowMs: 120,
            kind: "other",
            filePath: undefined,
            editorGroupId: undefined,
            focusReason: "no_active_text_editor",
        },
        createEpisodeId,
    );
    assert.equal(transition.effects[0].type, "doc_session_ended");
    assert.equal(transition.effects[0].filePath, "notes.md");
    assert.equal(transition.effects[1].type, "file_focus_changed");
    assert.equal(transition.effects[1].filePath, undefined);
});

test("stopping capture closes the stored documentation file", () => {
    const createEpisodeId = episodeFactory();
    const transition = observe(
        createCaptureActivityState(),
        { nowMs: 50, kind: "doc", filePath: "A.md", editorGroupId: 1 },
        createEpisodeId,
    );
    const stopped = stopCaptureActivity(
        transition.state,
        250,
        "client_stopped",
    );
    assert.deepEqual(stopped.effects, [
        {
            type: "doc_session_ended",
            filePath: "A.md",
            episodeId: "episode-1",
            segmentIndex: 0,
            durationMs: 200,
            closedBy: "client_stopped",
        },
    ]);
    assert.deepEqual(stopped.state, createCaptureActivityState());
});

test("same-file activity does not manufacture a focus transition", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "code", filePath: "main.rs", editorGroupId: 1 },
        createEpisodeId,
    );
    transition = observe(
        transition.state,
        {
            nowMs: 5,
            kind: "code",
            filePath: "main.rs",
            editorGroupId: 1,
            focusChanged: false,
        },
        createEpisodeId,
    );
    assert.deepEqual(transition.effects, []);
});

test("same-file code-to-doc activity starts a new episode without file focus", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "code", filePath: "mixed.md", editorGroupId: 1 },
        createEpisodeId,
    );
    transition = observe(
        transition.state,
        {
            nowMs: 25,
            kind: "doc",
            filePath: "mixed.md",
            editorGroupId: 1,
            focusChanged: false,
        },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["switch_pane", "doc_session_started"],
    );
    assert.equal(transition.effects[1].episodeId, "episode-1");
    assert.equal(transition.effects[1].startedBy, "activity_change");
});

test("moving the same file between editor groups records focus without splitting docs", () => {
    const createEpisodeId = episodeFactory();
    let transition = observe(
        createCaptureActivityState(),
        { nowMs: 0, kind: "doc", filePath: "notes.md", editorGroupId: 1 },
        createEpisodeId,
    );
    transition = observe(
        transition.state,
        { nowMs: 50, kind: "doc", filePath: "notes.md", editorGroupId: 2 },
        createEpisodeId,
    );
    assert.deepEqual(
        transition.effects.map((effect) => effect.type),
        ["file_focus_changed"],
    );
    assert.equal(transition.state.docSegment?.startedAtMs, 0);
    assert.equal(transition.state.docSegment?.episodeId, "episode-1");
});
