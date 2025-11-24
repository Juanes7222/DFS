import DashboardLayout from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Database } from "lucide-react";
import { toast } from "sonner";

export default function Storage() {
  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-foreground">Almacenamiento</h1>
          <p className="text-muted-foreground mt-2">
            Análisis y gestión del almacenamiento
          </p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="text-foreground">Próximamente</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col items-center justify-center py-12">
              <Database className="h-16 w-16 text-muted-foreground mb-4" />
              <p className="text-muted-foreground text-center">
                Las funciones de análisis y gestión del almacenamiento estarán disponibles próximamente.
              </p>
              <button
                onClick={() => toast.info("Función disponible próximamente")}
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
