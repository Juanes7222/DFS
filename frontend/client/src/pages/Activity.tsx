import DashboardLayout from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Activity as ActivityIcon } from "lucide-react";
import { toast } from "sonner";

export default function Activity() {
  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-foreground">Actividad</h1>
          <p className="text-muted-foreground mt-2">
            Actividad y registros del sistema
          </p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="text-foreground">Próximamente</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col items-center justify-center py-12">
              <ActivityIcon className="h-16 w-16 text-muted-foreground mb-4" />
              <p className="text-muted-foreground text-center">
                Las funciones de monitoreo y registro de actividades estarán disponibles próximamente.
              </p>
              <button
                onClick={() => toast.info("Feature coming soon")}
                className="mt-4 text-primary hover:underline"
              >
                Más información
              </button>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
