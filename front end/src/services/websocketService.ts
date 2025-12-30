import type { WebSocketEvent } from '../types/websocket';
import { WS_BASE_URL } from '../utils/constants';

export class WebSocketService {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  
  constructor(
    private jobId: string,
    private onMessage: (event: WebSocketEvent) => void,
    private onError?: (error: Event) => void,
    private onClose?: () => void
  ) {}
  
  connect(): void {
    const wsUrl = `${WS_BASE_URL}/ws/connect/${this.jobId}`;
    this.ws = new WebSocket(wsUrl);
    
    this.ws.onopen = () => {
      console.log('WebSocket connected');
      this.reconnectAttempts = 0;
    };
    
    this.ws.onmessage = (event) => {
      try {
        const data: WebSocketEvent = JSON.parse(event.data);
        this.onMessage(data);
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };
    
    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      this.onError?.(error);
    };
    
    this.ws.onclose = () => {
      console.log('WebSocket closed');
      this.onClose?.();
      this.attemptReconnect();
    };
  }
  
  private attemptReconnect(): void {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      setTimeout(() => {
        console.log(`Reconnecting... (attempt ${this.reconnectAttempts})`);
        this.connect();
      }, this.reconnectDelay * this.reconnectAttempts);
    }
  }
  
  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
  
  send(message: any): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }
}

