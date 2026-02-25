/**
 * Common type definitions for chat components
 */

export type Topic = "manuals" | "specifications" | "components";

export type WidgetType = "falchetti";

export interface IFalchettiWidgetData {
  all_cols: string[];
  filterable_cols: string[];
  distincts: Record<string, any[]>;
  rows: Record<string, any>[];
  max_rows: number;
}

export interface IWidgetPayload {
  type: WidgetType;
  data: IFalchettiWidgetData;
}

export interface IFileEntity {
  id: string;
  name: string;
  size: number;
  status?:
    | "pending"
    | "uploading"
    | "uploaded"
    | "error"
    | "deleting"
    | "processed";
  type: string;
  progress?: boolean;
  supportFileType?: string;
  createdDate?: number;
  originalFile?: File;
  uploadedId?: string;
  base64Url?: string;
  url?: string;
  error?: string;
  isRemote?: boolean;
}

export interface IChatItem {
  id: string;
  role?: string;
  content: string;

  // ✅ Add these
  messageType?: "text" | "widget";
  widget?: IWidgetPayload;
  
  isAnswer?: boolean;
  annotations?: any[];
  fileReferences?: Map<string, any>;
  duration?: number;
  message_files?: IFileEntity[];
  usageInfo?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
  more?: {
    time?: string;
  };
}

export interface ChatInputProps {
  onSubmit: (message: string) => void;
  isGenerating: boolean;
  currentUserMessage?: string;
}

export interface IAssistantMessageProps {
  message: IChatItem;
  agentLogo?: string;
  agentName?: string;
  loadingState?: "loading" | "streaming" | "none";
  showUsageInfo?: boolean;
  onDelete?: (messageId: string) => Promise<void>;
}

export interface IUserMessageProps {
  message: IChatItem;
  onEditMessage: (messageId: string) => void;
}

export interface ChatContextType {
  messageList: IChatItem[];
  isResponding: boolean;
  // Topic chosen at the start and kept for the session
  topic: Topic | null;
  // Call to set topic from the landing page buttons
  setTopic: (topic: Topic) => void;
  // Clears messages + topic (used by "New Chat")
  resetChat: () => void;
  // Normal send
  onSend: (message: string) => void | Promise<void>;
}

export interface AgentPreviewChatBotProps {
  agentName?: string;
  agentLogo?: string;
  chatContext: ChatContextType;
}

