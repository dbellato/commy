import { AgentPreview } from "./agents/AgentPreview";
import { ThemeProvider } from "./core/theme/ThemeProvider";

const App: React.FC = () => {
  // State to store the agent details
  const agentDetails ={
      // id: "chatbot",
      // object: "chatbot",
      // name: "Chatbot",
      id: "commy",
      object: "chatbot",
      name: "Commy",
      created_at: Date.now(),
      description: "This is commy, the Comacchio AI Agent.",
      model: "default",
      metadata: {
        logo: "Commy.svg",
      },
  };

  return (
    <ThemeProvider>
      <div className="app-container">
        <AgentPreview
          resourceId="sample-resource-id"
          agentDetails={agentDetails}
        />
      </div>
    </ThemeProvider>
  );
};

export default App;
