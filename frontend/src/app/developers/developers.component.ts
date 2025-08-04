import { Component, OnInit } from '@angular/core';
import { DeveloperService } from '../services/developer.service';
import { Developer } from '../models/Developer';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';

// Import nécessaires pour les charts
import { NgChartsModule } from 'ng2-charts';
import { ChartOptions, ChartType, ChartData } from 'chart.js';

@Component({
  selector: 'app-developers',
  standalone: true,
  imports: [
    CommonModule,
    HttpClientModule,
    FormsModule,
    NgChartsModule // Ajouté ici pour utiliser le composant chart
  ],
  styleUrls: ['./developers.component.css'], // attention au pluriel "styleUrls"
  templateUrl: './developers.component.html',
  providers: [DeveloperService]
})
export class DevelopersComponent implements OnInit {
  developers: Developer[] = [];
  filteredDevelopers: Developer[] = [];
  searchTerm: string = '';

  // Configuration chart
  public barChartOptions: ChartOptions = {
    responsive: true,
  };
  public barChartType: ChartType = 'bar';
  public barChartLegend = true;

  public barChartData: ChartData<'bar'> = {
    labels: [],
    datasets: [
      { data: [], label: 'Contributions (commits)' }
    ]
  };

  constructor(private developerService: DeveloperService) {}

  ngOnInit(): void {
    this.developerService.getAllDevelopers().subscribe({
      next: (data) => {
        this.developers = data;
        this.filteredDevelopers = data;
        this.updateChart(); // mettre à jour le graphique à l'init
      },
      error: (error) => {
        console.error('Erreur lors du chargement des développeurs :', error);
      }
    });
  }

  onSearchChange(): void {
    const term = this.searchTerm.toLowerCase().trim();
    this.filteredDevelopers = this.developers.filter(dev =>
      dev.author.toLowerCase().includes(term)
    );
    this.updateChart(); // mettre à jour le graphique à chaque recherche
  }

  updateChart(): void {
    this.barChartData.labels = this.filteredDevelopers.map(dev => dev.author);
    this.barChartData.datasets[0].data = this.filteredDevelopers.map(dev => dev.contributions || 0);
  }
}
