import { ReactNode, useState, useMemo } from "react";
import {
  Body1,
  Button,
  Caption1,
  Title2,
} from "@fluentui/react-components";
import { ChatRegular, MoreHorizontalRegular } from "@fluentui/react-icons";

import { AgentIcon } from "./AgentIcon";
import { SettingsPanel } from "../core/SettingsPanel";
import { AgentPreviewChatBot } from "./AgentPreviewChatBot";
import { MenuButton } from "../core/MenuButton/MenuButton";
import { IChatItem } from "./chatbot/types";
import { useRef, useEffect } from "react";
import styles from "./AgentPreview.module.css";
import type { Topic } from "./chatbot/types";


interface IAgent {
  id: string;
  object: string;
  created_at: number;
  name: string;
  description?: string | null;
  model: string;
  instructions?: string;
  tools?: Array<{ type: string }>;
  top_p?: number;
  temperature?: number;
  tool_resources?: {
    file_search?: {
      vector_store_ids?: string[];
    };
    [key: string]: any;
  };
  metadata?: Record<string, any>;
  response_format?: "auto" | string;
}

interface IAgentPreviewProps {
  resourceId: string;
  agentDetails: IAgent;
}


export function AgentPreview({ agentDetails }: IAgentPreviewProps): ReactNode {
  const [isSettingsPanelOpen, setIsSettingsPanelOpen] = useState(false);
  const [messageList, setMessageList] = useState<IChatItem[]>([]);
  const [isResponding, setIsResponding] = useState(false);
  const [topic, setTopic] = useState<Topic | null>(null);

  // Keep a ref so we never build "messages" from stale state ---> NEW
  const messageListRef = useRef<IChatItem[]>([]);
  useEffect(() => {
    messageListRef.current = messageList;
  }, [messageList]);

  // 2. Focus the input whenever a topic is selected
  useEffect(() => {
    if (!topic) return;
    requestAnimationFrame(() => {
      const editor = document.querySelector<HTMLElement>(
        '[data-testid="chat-input"]'
      );
      editor?.focus();
    });
  }, [topic]); // <-- fires when topic changes from null to a value

  // (Optional) if you want a consistent assistant id: ---> NEW
  const makeAssistantId = () => `assistant-${Date.now()}`;

  // Helper: update one message by id ---> NEW
  const updateMessageContentById = (id: string, content: string) => {
    setMessageList((prev) =>
      prev.map((m) => (m.id === id ? { ...m, content } : m))
    );
  };

  const replaceMessageById = (id: string, nextMsg: IChatItem) => {
    setMessageList((prev) => prev.map((m) => (m.id === id ? nextMsg : m)));
  };

  const handleSettingsPanelOpenChange = (isOpen: boolean) => {
    setIsSettingsPanelOpen(isOpen);
  };

  const newThread = () => {
    setMessageList([]);
    setTopic(null);
    deleteAllCookies();
  };

  const resetChat = () => {
    setMessageList([]);
    setTopic(null);
    deleteAllCookies();
  };

  const deleteAllCookies = (): void => {
    document.cookie.split(";").forEach((cookieStr: string) => {
      const trimmedCookieStr = cookieStr.trim();
      const eqPos = trimmedCookieStr.indexOf("=");
      const name =
        eqPos > -1 ? trimmedCookieStr.substring(0, eqPos) : trimmedCookieStr;
      document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/";
    });
  };

  const onSend = async (message: string) => {
    if (!topic) {
      // Optional: show an assistant message instead of silent fail
      const assistantId = makeAssistantId();
      const assistantMsg: IChatItem = {
        id: assistantId,
        content: "Please choose a topic first: Manuals, Specifications, or Components.",
        role: "assistant",
        isAnswer: true,
        more: { time: new Date().toISOString() },
      };
      setMessageList((prev) => [...prev, assistantMsg]);
      return;
    }

    const userMessage: IChatItem = {
      id: `user-${Date.now()}`,
      content: message,
      role: "user",
      isAnswer: false, // ✅ ensures it renders as UserMessage  ---> NEW
      more: { time: new Date().toISOString() },
    };

    // const assistantId = `assistant-${Date.now()}`;
    // const assistantMessage: IChatItem = {
    //   id: assistantId,
    //   role: "assistant",
    //   content: "",
    //   isAnswer: true, // ✅ important if UI filters on it
    //   more: { time: new Date().toISOString() },
    // };

    // ✅ Creation of the assistant  ---> NEW  
    const assistantId = makeAssistantId();
    const assistantPlaceholder: IChatItem = {
      id: assistantId,
      content: "",
      role: "assistant",
      isAnswer: true, // ✅ REQUIRED by AgentPreviewChatBot to render AssistantMessage
      more: { time: new Date().toISOString() },
    };


    // setMessageList((prev) => [...prev, userMessage]);
    // Add both bubbles immediately ---> NEW 
    setMessageList(prev => [...prev, userMessage, assistantPlaceholder]);
    setIsResponding(true);
    

    try {
    // Build the request from the latest state + the new user message,
    // but exclude the just-created empty assistant placeholder.

      // const messages = [...messageList, userMessage].map((item) => ({
      //   role: item.role,
      //   content: item.content,
      // }));
      // const postData = {messages};

      // NEW
      const history = [...messageListRef.current, userMessage]
        .filter((m) => m.content && m.content.trim().length > 0)
        .map((m) => ({
          role: m.role ?? (m.isAnswer ? "assistant" : "user"),
          content: m.content,
        }));
      const postData = { messages: history , topic };

      const response = await fetch("/chat", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(postData),
        credentials: "include", // <--- allow cookies to be included
      });

      // Log out the response status in case there’s an error
      console.log("[ChatClient] Response status:",response.status,response.statusText);
      // Log out the response headers in case there’s an error
      console.log("[ChatClient] content-type:",response.headers.get("content-type"));

      // If server returned e.g. 400 or 500, that’s not an exception, but we can check manually:
      // if (!response.ok) {
      //   console.error(
      //     "[ChatClient] The server has returned an error:",
      //     response.status,
      //     response.statusText
      //   );
      //   return;
      // }

      if (!response.ok) {
        const errText = await response.text();
        console.error("[ChatClient] Server error:", response.status, errText);
        updateMessageContentById(assistantId, errText || `Server error (${response.status})`); // ---> NEW
        return; // no setIsResponding(false) here
      }

      if (!response.body) {
        // throw new Error("ReadableStream not supported or response.body is null");
        updateMessageContentById(assistantId,"Response body is empty (stream not supported or server misconfigured)."); // ---> NEW
        return;
      }

      console.log("[ChatClient] Starting to handle streaming response...");
      // await handleMessages(response.body);
      await handleMessages(response.body, assistantId); // ---> NEW
    } catch (error: any) {
      // setIsResponding(false);
      // if (error.name === "AbortError") {
      //   console.log("[ChatClient] Fetch request aborted by user.");
      // } else {
      //   console.error("[ChatClient] Fetch failed:", error);
      // }
      console.error("[ChatClient] Fetch failed:", error);
      updateMessageContentById(assistantId,error?.message ?? "Request failed. Check console logs.");
    } finally {
      setIsResponding(false); // ✅ always runs
    }
  };

  // const handleMessages = (
  //   stream: ReadableStream<Uint8Array<ArrayBufferLike>>
  // ) => {
  const handleMessages = async (
    stream: ReadableStream<Uint8Array<ArrayBufferLike>>,
    assistantId: string // ---> NEW
  ): Promise<void> => {
    // let chatItem: IChatItem | null = null;
    let buffer = "";
    let accumulatedContent = "";
    let isStreaming = true;

    console.log("[ChatClient] handleMessages start");

    // Create a reader for the SSE stream
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    
    // const readStream = async () => {
    try { // ---> NEW
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          console.log("[ChatClient] SSE stream ended by server.");
          isStreaming = false; // ---> NEW
          return;
        }

        // const textChunk = decoder.decode(value, { stream: true });
        // buffer += textChunk;
        buffer += decoder.decode(value, { stream: true });

        // Process line-by-line
        let boundary = buffer.indexOf("\n");
        while (boundary !== -1) {
          // const chunk = buffer.slice(0, boundary).trim();
          const line = buffer.slice(0, boundary).replace(/\r$/, "");
          buffer = buffer.slice(boundary + 1);

          // Ignore empty/comment lines ---> NEW
          if (!line || line.startsWith(":")) {
            boundary = buffer.indexOf("\n");
            continue;
          }

          // if (chunk.startsWith("data: ")) {
          //   const jsonStr = chunk.slice(6);
          if (line.startsWith("data: ")) {
            // const jsonStr = line.slice(6);
            const jsonStr = line.slice(6).trim(); // ---> NEW

            // Some SSE servers send "data: [DONE]" /// ---> NEW
            if (jsonStr === "[DONE]") {
              isStreaming = false;
              return;
            }

            let data: any;
            try {
              data = JSON.parse(jsonStr);
            } catch (err) {
              console.error("[ChatClient] Failed to parse JSON:", jsonStr, err);
              boundary = buffer.indexOf("\n");
              continue;
            }

            // Handle error payloads
            // if (data.error) {
            //   if (!chatItem) chatItem = createAssistantMessageDiv();
            //   appendAssistantMessage(
            //     chatItem,
            //     data.error.message || "An error occurred.",
            //     false
            //   );
            //   throw new Error(data.error.message || "Stream error");
            // }
            if (data?.error) {
              const msg = data.error.message || "An error occurred.";
              updateMessageContentById(assistantId, msg);
              throw new Error(msg);
            }

            // End marker
            if (data?.type === "stream_end") {
              console.log("[ChatClient] Stream end marker received.");
              isStreaming = false; // ---> NEW
              return; // ✅ ends the whole handler cleanly
            }

            // ✅ NEW: Widget message (Falchetti, etc.)
            if (data?.type === "widget" && data?.widget) {
              replaceMessageById(assistantId, {
                id: assistantId,
                role: "assistant",
                content: "",          // not used for widgets
                isAnswer: true,
                messageType: "widget",
                widget: data.widget,  // { type: "falchetti", data: {...} }
                more: { time: new Date().toISOString() },
              });

              // stop reading; server will likely send stream_end anyway
              isStreaming = false;
              return;
            }

            // Your server uses these types
            // if (!chatItem) chatItem = createAssistantMessageDiv();
            if (data?.type === "completed_message") {
              // accumulatedContent = data.content;
              accumulatedContent = data.content ?? accumulatedContent;
              // clearAssistantMessage(chatItem);
              // isStreaming = false;
              isStreaming = false; // ---> NEW
            } else if (data?.type === "message") {
              accumulatedContent += data.content ?? "";
            // }
            // NEW
            } else if (typeof data?.content === "string") { 
              // fallback if server sends just {content:"..."}
              accumulatedContent += data.content;
            }
            // appendAssistantMessage(chatItem, accumulatedContent, isStreaming);

            // NEW: Update assistant bubble
            updateMessageContentById(assistantId, accumulatedContent);

            // NEW: Scroll when completed (optional)
            if (!isStreaming) {
              requestAnimationFrame(() => {
                const el = document.getElementById(`msg-${assistantId}`);
                el?.scrollIntoView({ behavior: "smooth", block: "end" });
              });
            }

          }

          boundary = buffer.indexOf("\n");
        }
      }
    // };
    // NEW
    } finally {
      try {
        reader.releaseLock();
      } catch {}
    }

  };

  const menuItems = [
    {
      key: "settings",
      children: "Settings",
      onClick: () => {
        setIsSettingsPanelOpen(true);
      },
    },
    {
      key: "terms",
      children: (
        <a
          className={styles.externalLink}
          href="https://aka.ms/aistudio/terms"
          target="_blank"
          rel="noopener noreferrer"
        >
          Terms of Use
        </a>
      ),
    },
    {
      key: "privacy",
      children: (
        <a
          className={styles.externalLink}
          href="https://go.microsoft.com/fwlink/?linkid=521839"
          target="_blank"
          rel="noopener noreferrer"
        >
          Privacy
        </a>
      ),
    },
    {
      key: "feedback",
      children: "Send Feedback",
      onClick: () => {
        // Handle send feedback click
        alert("Thank you for your feedback!");
      },
    },
  ];

  const chatContext = useMemo(
    () => ({
      messageList,
      isResponding,
      topic,
      setTopic,
      resetChat,
      onSend,
    }),
    [messageList, isResponding, topic]
  );

  return (
    <div className={styles.container}>
      <div className={styles.topBar}>
        <div className={styles.leftSection}>
          {messageList.length > 0 && (
            <>
              <AgentIcon
                alt=""
                iconClassName={styles.agentIcon}
                iconName={agentDetails.metadata?.logo}
              />
              <Body1 className={styles.agentName}>{agentDetails.name}</Body1>
            </>
          )}
        </div>
        <div className={styles.rightSection}>
          {" "}
          <Button
            appearance="subtle"
            icon={<ChatRegular aria-hidden={true} />}
            onClick={newThread}
          >
            Nuova Chat
          </Button>{" "}
          <MenuButton
            menuButtonText=""
            menuItems={menuItems}
            menuButtonProps={{
              appearance: "subtle",
              icon: <MoreHorizontalRegular />,
              "aria-label": "Settings",
            }}
          />
        </div>
      </div>
      <div className={styles.content}>          <>
        {messageList.length === 0 && (
          <div className={styles.emptyChatContainer}>
            <AgentIcon
              alt=""
              iconClassName={styles.emptyStateAgentIcon}
              iconName={agentDetails.metadata?.logo}
            />
            <Caption1 className={styles.agentName}>{agentDetails.name}</Caption1>
            <Title2>Come posso aiutarti oggi?</Title2>

            {!topic && (
              <div style={{ display: "flex", gap: 12, marginTop: 16, flexWrap: "wrap", justifyContent: "center" }}>
                <Button onClick={() => setTopic("manuals")}>Manuali</Button>
                <Button onClick={() => setTopic("specifications")}>Specifiche</Button>
                <Button onClick={() => setTopic("components")}>Componenti</Button>
              </div>
            )}

            {topic && (
              <div style={{ marginTop: 12, opacity: 0.75 }}>
                Argomento selezionato: <b>{topic}</b>. Scrivi la tua richiesta...
              </div>
            )}
          </div>
        )}
        <AgentPreviewChatBot
          agentName={agentDetails.name}
          agentLogo={agentDetails.metadata?.logo}
          chatContext={chatContext}            />          </>
      </div>

      {/* Settings Panel */}
      <SettingsPanel
        isOpen={isSettingsPanelOpen}
        onOpenChange={handleSettingsPanelOpenChange}
      />
    </div>
  );
}
