import { useEffect, useRef } from 'react';
import { WebSocketService } from '../services/websocketService';
import { useAppStore } from '../stores/useAppStore';
import type { WebSocketEvent, StatusTickEvent, APIRequestStartEvent, APIResponseSuccessEvent, StepStartEvent, StepCompleteEvent } from '../types/websocket';

export const useWebSocket = (jobId: string | null) => {
  const wsServiceRef = useRef<WebSocketService | null>(null);
  const { addStatusTick, setLatestStatusMessage } = useAppStore();
  
  useEffect(() => {
    if (jobId) {
      console.log('useWebSocket: jobId changed to:', jobId);
    } else {
      console.log('useWebSocket: jobId is null, not connecting');
    }
  }, [jobId]);
  
  useEffect(() => {
    if (!jobId) return;
    
    const handleMessage = (event: WebSocketEvent) => {
      console.log('WebSocket message received:', event.type, event);
      
      switch (event.type) {
        case 'status_tick':
          const tickEvent = event as StatusTickEvent;
          console.log('Processing status_tick event:', tickEvent.data);
          addStatusTick(tickEvent.data);
          setLatestStatusMessage(tickEvent.data.message);
          break;
        case 'step_start':
          {
            const startEvent = event as StepStartEvent;
            const data = startEvent.data;
            const startTick = {
              job_id: data.job_id,
              seq: data.seq,
              ts: data.ts,
              phase: data.phase,
              step: data.step,
              step_name: data.step_name,
              scope: data.scope,
              message: `▶️ ${data.message}`,
              level: 'info' as const,
              summary: data.summary
            };
            addStatusTick(startTick);
            setLatestStatusMessage(data.message);
          }
          break;
        case 'step_complete':
          {
            const completeEvent = event as StepCompleteEvent;
            const data = completeEvent.data;
            const completeTick = {
              job_id: data.job_id,
              seq: data.seq,
              ts: data.ts,
              phase: data.phase,
              step: data.step,
              step_name: data.step_name,
              scope: data.scope,
              message: `OK: ${data.message}`,
              level: 'info' as const,
              summary: data.summary
            };
            addStatusTick(completeTick);
            setLatestStatusMessage(data.message);
          }
          break;
          
        case 'api_request_start':
          const requestEvent = event as APIRequestStartEvent;
          console.log('Processing api_request_start event:', requestEvent.data);
          const requestTick = {
            job_id: requestEvent.data.job_id,
            seq: requestEvent.data.seq,
            ts: requestEvent.data.ts,
            phase: requestEvent.data.phase,
            step: requestEvent.data.step,
            step_name: requestEvent.data.step_name,
            scope: requestEvent.data.scope,
            message: requestEvent.data.message,
            level: 'info' as const
          };
          addStatusTick(requestTick);
          setLatestStatusMessage(requestEvent.data.message);
          break;
          
        case 'api_response_success':
          const responseEvent = event as APIResponseSuccessEvent;
          console.log('Processing api_response_success event:', responseEvent.data);
          const responseTick = {
            job_id: responseEvent.data.job_id,
            seq: responseEvent.data.seq,
            ts: responseEvent.data.ts,
            phase: responseEvent.data.phase,
            step: responseEvent.data.step,
            step_name: responseEvent.data.step_name,
            scope: responseEvent.data.scope,
            message: responseEvent.data.message,
            level: 'info' as const
          };
          addStatusTick(responseTick);
          setLatestStatusMessage(responseEvent.data.message);
          break;
          
        case 'phase_complete':
          console.log('Phase complete event:', event);
          // Handle phase completion
          break;
          
        case 'error':
          console.error('Error event received:', event);
          const errorEvent = event as any;
          if (errorEvent.data?.message) {
            setLatestStatusMessage(`Error: ${errorEvent.data.message}`);
          }
          break;
          
        default:
          console.warn('Unknown WebSocket event type:', event);
      }
    };
    
    console.log('Creating WebSocket service for jobId:', jobId);
    wsServiceRef.current = new WebSocketService(
      jobId,
      handleMessage,
      (error) => {
        console.error('WebSocket error:', error);
      },
      () => {
        console.log('WebSocket closed');
      }
    );
    
    console.log('Connecting WebSocket...');
    wsServiceRef.current.connect();
    
    return () => {
      wsServiceRef.current?.disconnect();
    };
  }, [jobId, addStatusTick, setLatestStatusMessage]);
};

