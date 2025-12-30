export interface Scope {
  entity?: string | null;
  attribute?: string | null;
  relation?: string | null;
}

export interface APICallInfo {
  estimated_tokens?: number;
  tokens_used?: number;
  response_time_ms?: number;
  call_type?: string;
}

export interface APIRequestStartEvent {
  type: "api_request_start";
  data: {
    job_id: string;
    seq: number;
    ts: string;
    phase: number;
    step: string;
    step_name: string;
    step_id: string;
    scope: Scope;
    api_call: APICallInfo;
    message: string;
  };
}

export interface APIResponseSuccessEvent {
  type: "api_response_success";
  data: {
    job_id: string;
    seq: number;
    ts: string;
    phase: number;
    step: string;
    step_name: string;
    step_id: string;
    scope: Scope;
    api_call: APICallInfo;
    message: string;
  };
}

export interface StatusTickEvent {
  type: "status_tick";
  data: {
    job_id: string;
    seq: number;
    ts: string;
    phase: number;
    step: string;
    step_name: string;
    scope: Scope;
    message: string;
    level: "info" | "warning" | "error";
  };
}

export interface StepLifecycleData {
  job_id: string;
  seq: number;
  ts: string;
  phase: number;
  step: string;
  step_name: string;
  step_id: string;
  scope: Scope;
  message: string;
  summary?: Record<string, any>;
}

export interface StepStartEvent {
  type: "step_start";
  data: StepLifecycleData;
}

export interface StepCompleteEvent {
  type: "step_complete";
  data: StepLifecycleData;
}

export type WebSocketEvent =
  | APIRequestStartEvent
  | APIResponseSuccessEvent
  | StatusTickEvent
  | StepStartEvent
  | StepCompleteEvent
  | { type: "connected"; data: { job_id: string; message: string } }
  | { type: "phase_complete"; data: { phase: number; results: any } }
  | { type: "error"; data: { message: string; error_type: string } };

