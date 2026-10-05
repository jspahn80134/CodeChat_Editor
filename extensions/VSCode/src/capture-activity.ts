// Copyright (C) 2026 Bryan A. Jones.
//
// This file is part of the CodeChat Editor.

export type CaptureActivityKind = "doc" | "code" | "other";

export interface CaptureDocSegment {
    startedAtMs: number;
    filePath: string;
    fileKey: string;
    episodeId: string;
    segmentIndex: number;
}

export interface CaptureActivityState {
    activityKind: CaptureActivityKind;
    focusedFilePath?: string;
    focusedFileKey?: string;
    editorGroupId?: number;
    docSegment?: CaptureDocSegment;
}

export interface CaptureActivityObservation {
    nowMs: number;
    kind: CaptureActivityKind;
    filePath?: string;
    fileKey?: string;
    editorGroupId?: number;
    focusChanged: boolean;
    focusReason?: string;
}

export type CaptureActivityEffect =
    | {
          type: "file_focus_changed";
          filePath?: string;
          from: CaptureActivityKind;
          to: CaptureActivityKind;
          reason: string;
          editorGroupId?: number;
          previousEditorGroupId?: number;
          fileChanged: boolean;
          editorGroupChanged: boolean;
      }
    | {
          type: "switch_pane";
          filePath?: string;
          from: "doc" | "code";
          to: "doc" | "code";
      }
    | {
          type: "doc_session_started";
          filePath: string;
          episodeId: string;
          segmentIndex: number;
          startedBy: string;
      }
    | {
          type: "doc_session_ended";
          filePath: string;
          episodeId: string;
          segmentIndex: number;
          durationMs: number;
          closedBy: string;
      };

export interface CaptureActivityTransition {
    state: CaptureActivityState;
    effects: CaptureActivityEffect[];
}

export function createCaptureActivityState(): CaptureActivityState {
    return { activityKind: "other" };
}

function isDocOrCode(kind: CaptureActivityKind): kind is "doc" | "code" {
    return kind === "doc" || kind === "code";
}

function endDocSegment(
    segment: CaptureDocSegment,
    nowMs: number,
    closedBy: string,
): CaptureActivityEffect {
    return {
        type: "doc_session_ended",
        filePath: segment.filePath,
        episodeId: segment.episodeId,
        segmentIndex: segment.segmentIndex,
        durationMs: Math.max(0, nowMs - segment.startedAtMs),
        closedBy,
    };
}

export function transitionCaptureActivity(
    state: CaptureActivityState,
    observation: CaptureActivityObservation,
    createEpisodeId: () => string,
): CaptureActivityTransition {
    const effects: CaptureActivityEffect[] = [];
    const fileChanged =
        state.focusedFileKey !== observation.fileKey ||
        (state.focusedFilePath === undefined) !==
            (observation.filePath === undefined);
    const editorGroupChanged =
        state.editorGroupId !== observation.editorGroupId;
    const focusContextChanged =
        observation.focusChanged && (fileChanged || editorGroupChanged);

    let continuedEpisode:
        { episodeId: string; segmentIndex: number } | undefined;
    let docSegment = state.docSegment;
    if (
        docSegment !== undefined &&
        (observation.kind !== "doc" || fileChanged)
    ) {
        const closedBy = fileChanged
            ? "file_focus_changed"
            : observation.kind === "code"
              ? "switch_to_code"
              : "activity_change";
        effects.push(endDocSegment(docSegment, observation.nowMs, closedBy));
        if (
            fileChanged &&
            state.activityKind === "doc" &&
            observation.kind === "doc"
        ) {
            continuedEpisode = {
                episodeId: docSegment.episodeId,
                segmentIndex: docSegment.segmentIndex + 1,
            };
        }
        docSegment = undefined;
    }

    if (focusContextChanged) {
        effects.push({
            type: "file_focus_changed",
            filePath: observation.filePath,
            from: state.activityKind,
            to: observation.kind,
            reason: observation.focusReason ?? "active_editor_changed",
            editorGroupId: observation.editorGroupId,
            previousEditorGroupId: state.editorGroupId,
            fileChanged,
            editorGroupChanged,
        });
    }

    if (
        isDocOrCode(state.activityKind) &&
        isDocOrCode(observation.kind) &&
        observation.kind !== state.activityKind
    ) {
        effects.push({
            type: "switch_pane",
            filePath: observation.filePath,
            from: state.activityKind,
            to: observation.kind,
        });
    }

    if (
        observation.kind === "doc" &&
        docSegment === undefined &&
        observation.filePath !== undefined &&
        observation.fileKey !== undefined
    ) {
        const episode = continuedEpisode ?? {
            episodeId: createEpisodeId(),
            segmentIndex: 0,
        };
        docSegment = {
            startedAtMs: observation.nowMs,
            filePath: observation.filePath,
            fileKey: observation.fileKey,
            episodeId: episode.episodeId,
            segmentIndex: episode.segmentIndex,
        };
        effects.push({
            type: "doc_session_started",
            filePath: observation.filePath,
            episodeId: episode.episodeId,
            segmentIndex: episode.segmentIndex,
            startedBy: fileChanged ? "file_focus_changed" : "activity_change",
        });
    }

    return {
        state: {
            activityKind: observation.kind,
            focusedFilePath: observation.focusChanged
                ? observation.filePath
                : state.focusedFilePath,
            focusedFileKey: observation.focusChanged
                ? observation.fileKey
                : state.focusedFileKey,
            editorGroupId: observation.focusChanged
                ? observation.editorGroupId
                : state.editorGroupId,
            docSegment,
        },
        effects,
    };
}

export function stopCaptureActivity(
    state: CaptureActivityState,
    nowMs: number,
    closedBy: string,
): CaptureActivityTransition {
    const effects =
        state.docSegment === undefined
            ? []
            : [endDocSegment(state.docSegment, nowMs, closedBy)];
    return {
        state: createCaptureActivityState(),
        effects,
    };
}
